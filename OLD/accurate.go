//go:build !js

package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/bits"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"gonum.org/v1/gonum/dsp/fourier"
)

// ===================== CONFIG =====================
var (
	SampleRate      = 11025        // Hz (low sr speeds up FFT & reduces noise)
	FFTSize         = 2048         // power of two
	HopSize         = 512          // STFT hop
	MinFreqHz       = 80           // ignore sub-bass rumble
	MaxFreqHz       = 5000         // limit to speech/music core band
	PeakAreaTime    = 8            // +/- frames neighborhood for local maxima
	PeakAreaFreq    = 8            // +/- bins neighborhood for local maxima
	PeakAbsThresh   = 6.0          // dBFS magnitude threshold; tune if too sparse/dense
	FanoutMinDelta  = 1            // frames
	FanoutMaxDelta  = 32           // frames (how far ahead to form pairs)
	FanoutTargets   = 10           // limit number of target peaks per anchor (speeds indexing)
	Workers         = max(2, runtime.NumCPU()-1)
)

// ===================== TYPES =====================

type Posting struct {
	TrackID uint32
	Offset  uint32 // frame index of anchor in track
}

type Index struct {
	// Hash -> postings (trackID, offset)
	H map[uint64][]Posting
	// TrackID -> metadata
	Tracks []TrackMeta
	// For fast lookup of ID by path
	TrackIDByPath map[string]uint32
}

type TrackMeta struct {
	ID   uint32
	Path string
	DurS float64
	Title  string
	Artist string
	Album  string
	Track  string
}

// ===================== FFT Provider (CPU baseline, GPU-ready hook) =====================

type FFTProvider interface {
	New(n int) FFT
}

type FFT interface {
	// Compute power spectrum of real input frame (len = n). Returns magnitude squared per bin (0..n/2).
	PowerReal(frame []float32) []float64
}

type CPUFFTProvider struct{}

type cpuFFT struct {
	r2c *fourier.FFT
	N   int
	buf []float64
}

func (p *CPUFFTProvider) New(n int) FFT {
	return &cpuFFT{r2c: fourier.NewFFT(n), N: n, buf: make([]float64, n)}
}

func (f *cpuFFT) PowerReal(frame []float32) []float64 {
	for i := 0; i < f.N; i++ {
		if i < len(frame) {
			f.buf[i] = float64(frame[i])
		} else {
			f.buf[i] = 0
		}
	}
	coeffs := f.r2c.Coefficients(nil, f.buf)
	// return only 0..N/2
	N2 := f.N/2 + 1
	out := make([]float64, N2)
	for k := 0; k < N2; k++ {
		out[k] = real(coeffs[k])*real(coeffs[k]) + imag(coeffs[k])*imag(coeffs[k])
	}
	return out
}

// ===================== DECODING =====================

// decodeToMonoF32 uses ffmpeg to decode any input (e.g., m4a) to mono float32 PCM at SampleRate.
func decodeToMonoF32(ctx context.Context, path string) ([]float32, error) {
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-v", "error",
		"-i", path,
		"-t", "180", // limit decode length to 180 seconds
		"-ac", "1",
		"-ar", fmt.Sprint(SampleRate),
		"-f", "f32le",
		"pipe:1",
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("ffmpeg decode failed: %w", err)
	}
	b := out.Bytes()
	if len(b)%4 != 0 { // float32
		return nil, errors.New("unexpected ffmpeg float32le length")
	}
	sz := len(b) / 4
	samples := make([]float32, sz)
	for i := 0; i < sz; i++ {
		bits := uint32(b[4*i]) | uint32(b[4*i+1])<<8 | uint32(b[4*i+2])<<16 | uint32(b[4*i+3])<<24
		samples[i] = math.Float32frombits(bits)
	}
	return samples, nil
}

// ===================== METADATA =====================

type FFProbeFormat struct {
	Tags map[string]string `json:"tags"`
	Duration string        `json:"duration"`
}

type FFProbeResult struct {
	Format FFProbeFormat `json:"format"`
}

func probeMetadata(ctx context.Context, path string) (title, artist, album, track string, durS float64, err error) {
	cmd := exec.CommandContext(ctx, "ffprobe",
		"-v", "error",
		"-show_entries", "format=duration:format_tags=title,artist,album,track",
		"-of", "json", path,
	)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", "", "", "", 0, err
	}
	var result FFProbeResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		return "", "", "", "", 0, err
	}
	tags := result.Format.Tags
	d, _ := strconv.ParseFloat(result.Format.Duration, 64)
	return tags["title"], tags["artist"], tags["album"], tags["track"], d, nil
}

// ===================== STFT & PEAKS =====================

type Peak struct {
	T int // frame index
	F int // bin index
	A float64 // amplitude in dBFS
}

func hann(n int) []float32 {
	w := make([]float32, n)
	for i := 0; i < n; i++ {
		w[i] = float32(0.5 * (1 - math.Cos(2*math.Pi*float64(i)/float64(n))))
	}
	return w
}

func clamp(x, lo, hi int) int { if x < lo { return lo }; if x > hi { return hi }; return x }

func stftPeaks(samples []float32, fftProv FFTProvider) ([][]float64, []Peak) {
	// Precompute window
	win := hann(FFTSize)
	// frames
	if len(samples) < FFTSize {
		return nil, nil
	}
	frames := 1 + (len(samples)-FFTSize)/HopSize
	// Guard: skip absurdly long tracks
	if frames > 20000 {
		return nil, nil
	}
	fft := fftProv.New(FFTSize)
	N2 := FFTSize/2 + 1
	// freq range bins
	minBin := int(float64(MinFreqHz) / float64(SampleRate) * float64(FFTSize))
	maxBin := int(float64(MaxFreqHz) / float64(SampleRate) * float64(FFTSize))
	minBin = clamp(minBin, 1, N2-1)
	maxBin = clamp(maxBin, minBin+1, N2-1)
	// spectrogram in dBFS
	S := make([][]float64, frames)
	maxPow := 1e-12
	for t := 0; t < frames; t++ {
		start := t * HopSize
		frame := make([]float32, FFTSize)
		for i := 0; i < FFTSize; i++ {
			frame[i] = samples[start+i] * win[i]
		}
		P := fft.PowerReal(frame)
		// track global max for dB scaling
		for k := minBin; k <= maxBin; k++ {
			if P[k] > maxPow {
				maxPow = P[k]
			}
		}
		S[t] = P
	}
	ref := maxPow
	for t := 0; t < frames; t++ {
		for k := 0; k < len(S[t]); k++ {
			if S[t][k] < 1e-12 {
				S[t][k] = 1e-12
			}
			S[t][k] = 10 * math.Log10(S[t][k]/ref)
		}
	}
	// peak picking: keep top 30 peaks per frame in [minBin, maxBin]
	peaks := make([]Peak, 0, frames*10)
	for t := 0; t < frames; t++ {
		type pk struct {
			K   int
			Mag float64
		}
		framePeaks := make([]pk, 0, maxBin-minBin+1)
		for k := minBin; k <= maxBin; k++ {
			mag := S[t][k]
			// local maximum check within neighborhood
			isMax := true
			for dt := -PeakAreaTime; dt <= PeakAreaTime && isMax; dt++ {
				tt := t + dt
				if tt < 0 || tt >= frames {
					continue
				}
				for df := -PeakAreaFreq; df <= PeakAreaFreq; df++ {
					kk := k + df
					if kk < 0 || kk >= N2 {
						continue
					}
					if dt == 0 && df == 0 {
						continue
					}
					if S[tt][kk] > mag {
						isMax = false
						break
					}
				}
			}
			if isMax {
				framePeaks = append(framePeaks, pk{K: k, Mag: mag})
			}
		}
		// Sort and keep top 30 peaks by magnitude
		sort.Slice(framePeaks, func(i, j int) bool { return framePeaks[i].Mag > framePeaks[j].Mag })
		limit := 30
		if len(framePeaks) < limit {
			limit = len(framePeaks)
		}
		for i := 0; i < limit; i++ {
			peaks = append(peaks, Peak{T: t, F: framePeaks[i].K, A: framePeaks[i].Mag})
		}
	}
	return S, peaks
}

// ===================== HASHING =====================
// Hash packs (f1,f2,dt) into 64 bits: f1(16) | f2(16) | dt(16) | reserve(16)

func packHash(f1, f2, dt uint32) uint64 {
	return (uint64(f1&0xFFFF) << 48) | (uint64(f2&0xFFFF) << 32) | (uint64(dt&0xFFFF) << 16)
}

func buildHashes(peaks []Peak) map[uint64][]int {
	// Build a map: hash -> list of anchor frame indices in this track
	byTime := make(map[int][]Peak)
	for _, p := range peaks {
		byTime[p.T] = append(byTime[p.T], p)
	}
	// precompute a sparse list of times having peaks
	ts := make([]int, 0, len(byTime))
	for t := range byTime {
		ts = append(ts, t)
	}
	sort.Ints(ts)
	res := make(map[uint64][]int)
	for _, t := range ts {
		anchors := byTime[t]
		for _, a := range anchors {
			// collect candidate targets in (t+FanoutMinDelta ... t+FanoutMaxDelta)
			cands := make([]Peak, 0, 16)
			for dt := FanoutMinDelta; dt <= FanoutMaxDelta; dt++ {
				tt := t + dt
				ps, ok := byTime[tt]
				if !ok {
					continue
				}
				cands = append(cands, ps...)
			}
			if len(cands) == 0 {
				continue
			}
			// pick top-N by amplitude
			sort.Slice(cands, func(i, j int) bool { return cands[i].A > cands[j].A })
			limit := FanoutTargets
			if limit > len(cands) {
				limit = len(cands)
			}
			for i := 0; i < limit; i++ {
				b := cands[i]
				dt := b.T - a.T
				if dt <= 0 {
					continue
				}
				// Quantize f1, f2, dt before packing
				f1q := uint32(a.F / 2)
				f2q := uint32(b.F / 2)
				dtq := uint32(dt / 2)
				key := packHash(f1q, f2q, dtq)
				res[key] = append(res[key], a.T)
			}
		}
	}
	return res
}

// ===================== INDEXING =====================

func indexFolder(ctx context.Context, folder string) (*Index, error) {
	idx := &Index{H: make(map[uint64][]Posting), TrackIDByPath: make(map[string]uint32)}
	paths := []string{}
	err := filepath.WalkDir(folder, func(path string, d os.DirEntry, err error) error {
		if err != nil { return err }
		if d.IsDir() { return nil }
		ext := strings.ToLower(filepath.Ext(d.Name()))
		if ext == ".m4a" || ext == ".mp4" || ext == ".aac" || ext == ".mp3" { // allow a few
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil { return nil, err }
	if len(paths) == 0 { return nil, fmt.Errorf("no audio files found in %s", folder) }

	fmt.Printf("Found %d files. Indexing with %d workers...\n", len(paths), Workers)
	// watchdog: periodic status print
	var processed, skipped int64
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			total := atomic.LoadInt64(&processed) + atomic.LoadInt64(&skipped)
			fmt.Printf("[WATCHDOG] %d/%d done (processed=%d skipped=%d)\n", total, len(paths), atomic.LoadInt64(&processed), atomic.LoadInt64(&skipped))
		}
	}()
	fftProv := &CPUFFTProvider{}
	type result struct {
		id   uint32
		path string
		durs float64
		h    map[uint64][]int
		meta TrackMeta
		err  error
	}
	jobs := make(chan string, len(paths))
	out := make(chan result, len(paths))
	var wg sync.WaitGroup

	for w := 0; w < Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				func() {
					defer func() {
						if r := recover(); r != nil {
							out <- result{path: p, err: fmt.Errorf("panic: %v", r)}
						}
					}()
					done := make(chan result, 1)
					go func() {
						// Create a job context with a timeout of 3 minutes
						jobCtx, jobCancel := context.WithTimeout(ctx, 3*time.Minute)
						defer jobCancel()
						fmt.Printf("[DEBUG] start %s\n", p)
						fmt.Printf("[DEBUG] start decode %s\n", p)
						// Decode context: 2 min timeout
						ctxDec, cancelDec := context.WithTimeout(jobCtx, 2*time.Minute)
						samps, err := decodeToMonoF32(ctxDec, p)
						cancelDec()
						if err != nil {
							done <- result{path: p, err: err}
							return
						}
						fmt.Printf("[DEBUG] decoded %s (samples=%d)\n", p, len(samps))
						fmt.Printf("[DEBUG] start probe %s\n", p)
						// Probe metadata context: 30s timeout
						ctxMeta, cancelMeta := context.WithTimeout(jobCtx, 30*time.Second)
						title, artist, album, track, du, metaErr := probeMetadata(ctxMeta, p)
						cancelMeta()
						fmt.Printf("[DEBUG] probed %s title=%s artist=%s\n", p, title, artist)
						if metaErr != nil {
							fmt.Printf("[WARN] probe failed for %s: %v\n", p, metaErr)
						}
						// Analysis with timeout (peaks + hashes)
						var h map[uint64][]int
						analysisDone := make(chan struct{})
						go func() {
							fmt.Printf("[DEBUG] start peaks %s\n", p)
							_, peaks := stftPeaks(samps, fftProv)
							fmt.Printf("[DEBUG] start hashes %s\n", p)
							h = buildHashes(peaks)
							close(analysisDone)
						}()
						select {
						case <-analysisDone:
							fmt.Printf("[DEBUG] done %s\n", p)
							done <- result{path: p, durs: du, h: h, meta: TrackMeta{Path: p, DurS: du, Title: title, Artist: artist, Album: album, Track: track}}
						case <-time.After(30 * time.Second):
							done <- result{path: p, err: fmt.Errorf("analysis timeout")}
							return
						}
					}()
					select {
					case r := <-done:
						out <- r
					case <-time.After(2 * time.Minute):
						out <- result{path: p, err: fmt.Errorf("job timeout")}
					}
				}()
			}
		}()
	}
	for i, p := range paths {
		fmt.Printf("[ENQUEUE] %d/%d %s\n", i+1, len(paths), p)
		jobs <- p
	}
	close(jobs)
	go func() {
		wg.Wait()
		close(out)
	}()

	for r := range out {
		if r.err != nil {
			atomic.AddInt64(&skipped, 1)
			fmt.Printf("[ERROR] %s -> %v\n", r.path, r.err)
		} else {
			atomic.AddInt64(&processed, 1)
			meta := r.meta
			meta.ID = uint32(len(idx.Tracks))
			idx.Tracks = append(idx.Tracks, meta)
			idx.TrackIDByPath[meta.Path] = meta.ID
			for key, offsets := range r.h {
				posts := idx.H[key]
				for _, off := range offsets {
					posts = append(posts, Posting{TrackID: meta.ID, Offset: uint32(off)})
				}
				idx.H[key] = posts
			}
		}
		total := atomic.LoadInt64(&processed) + atomic.LoadInt64(&skipped)
		fmt.Printf("[%d/%d] processed=%d skipped=%d\r", total, len(paths), atomic.LoadInt64(&processed), atomic.LoadInt64(&skipped))
	}
	fmt.Printf("\n")
	fmt.Printf("Indexed %d tracks (%d skipped), %d unique hashes.\n", len(idx.Tracks), atomic.LoadInt64(&skipped), len(idx.H))
	return idx, nil
}

// ===================== MATCHING =====================

type Match struct {
	TrackID uint32
	Track   TrackMeta
	Score   int     // vote count at best delta
	Delta   int     // frames
	Conf    float64 // Score normalized by query hash count
}

func matchQuery(ctx context.Context, idx *Index, queryPath string) ([]Match, error) {
	samps, err := decodeToMonoF32(ctx, queryPath)
	if err != nil { return nil, err }
	fftProv := &CPUFFTProvider{}
	_, peaks := stftPeaks(samps, fftProv)
	qh := buildHashes(peaks)
	votes := make(map[uint32]map[int]int) // trackID -> delta -> count
	var totalQ int
	for key, qOffsets := range qh {
		totalQ += len(qOffsets)
		posts, ok := idx.H[key]
		if !ok { continue }
		// For each collision, compute delta = trackOffset - queryAnchorOffset
		for _, qoff := range qOffsets {
			for _, post := range posts {
				delta := int(post.Offset) - qoff
				m := votes[post.TrackID]
				if m == nil { m = make(map[int]int); votes[post.TrackID] = m }
				m[delta]++
			}
		}
	}
	// For each track, pick the delta with the highest votes
	matches := make([]Match, 0, len(votes))
	for tid, m := range votes {
		bestDelta, best := 0, 0
		for d, c := range m {
			if c > best { best = c; bestDelta = d }
		}
		meta := idx.Tracks[tid]
		conf := float64(best) / float64(max(1, totalQ))
		matches = append(matches, Match{TrackID: tid, Track: meta, Score: best, Delta: bestDelta, Conf: conf})
	}

	sort.Slice(matches, func(i, j int) bool {
		if matches[i].Score == matches[j].Score {
			return matches[i].Conf > matches[j].Conf
		}
		return matches[i].Score > matches[j].Score
	})
	return matches, nil
}

// ===================== PERSISTENCE =====================

func saveIndex(path string, idx *Index) error {
	f, err := os.Create(path)
	if err != nil { return err }
	defer f.Close()
	bw := bufio.NewWriter(f)
	zw := gzip.NewWriter(bw)
	enc := gob.NewEncoder(zw)
	if err := enc.Encode(idx); err != nil { return err }
	if err := zw.Close(); err != nil { return err }
	return bw.Flush()
}

func loadIndex(path string) (*Index, error) {
	f, err := os.Open(path)
	if err != nil { return nil, err }
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil { return nil, err }
	dec := gob.NewDecoder(zr)
	var idx Index
	if err := dec.Decode(&idx); err != nil { return nil, err }
	return &idx, nil
}

// ===================== UTIL =====================

func max(a, b int) int { if a > b { return a }; return b }

func nextPow2(n int) int { if n <= 1 { return 1 }; return 1 << (bits.Len(uint(n-1))) }

// ===================== MAIN =====================

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `Shazam-style audio fingerprinting (Go)\n\n`)
		fmt.Fprintf(os.Stderr, `Commands:\n`)
		fmt.Fprintf(os.Stderr, `  index <folder> <index.gob.gz>\n`)
		fmt.Fprintf(os.Stderr, `  query <index.gob.gz> <query.m4a>\n\n`)
	}
	flag.Parse()
	if flag.NArg() < 1 { flag.Usage(); os.Exit(2) }
	sub := flag.Arg(0)
	switch sub {
	case "index":
		if flag.NArg() != 3 { flag.Usage(); os.Exit(2) }
		folder := flag.Arg(1)
		out := flag.Arg(2)
		ctx := context.Background()
		start := time.Now()
		idx, err := indexFolder(ctx, folder)
		if err != nil { fmt.Fprintln(os.Stderr, "index error:", err); os.Exit(1) }
		if err := saveIndex(out, idx); err != nil { fmt.Fprintln(os.Stderr, "save error:", err); os.Exit(1) }
		fmt.Printf("Saved index to %s (tracks=%d, hashes=%d) in %v\n", out, len(idx.Tracks), len(idx.H), time.Since(start))
	case "query":
		if flag.NArg() != 3 { flag.Usage(); os.Exit(2) }
		idxPath := flag.Arg(1)
		queryPath := flag.Arg(2)
		idx, err := loadIndex(idxPath)
		if err != nil { fmt.Fprintln(os.Stderr, "load error:", err); os.Exit(1) }
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		start := time.Now()
		matches, err := matchQuery(ctx, idx, queryPath)
		if err != nil { fmt.Fprintln(os.Stderr, "match error:", err); os.Exit(1) }
		fmt.Printf("Top matches for %s (took %v):\n", filepath.Base(queryPath), time.Since(start))
		for i, m := range matches {
			if i >= 10 { break }
			secOffset := float64(m.Delta*HopSize) / float64(SampleRate)
			fmt.Printf("%2d. score=%4d conf=%.3f delta=%.2fs  %s – %s (%s)\n", i+1, m.Score, m.Conf, secOffset, m.Track.Artist, m.Track.Title, m.Track.Album)
		}
	default:
		flag.Usage()
		os.Exit(2)
	}
}
