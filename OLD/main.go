// //go:build !js

package main

// import (
// 	"bufio"
// 	"bytes"
// 	"compress/gzip"
// 	"context"
// 	"encoding/gob"
// 	"encoding/json"
// 	"errors"
// 	"flag"
// 	"fmt"
// 	"math"
// 	"math/bits"
// 	"os"
// 	"os/exec"
// 	"path/filepath"
// 	"runtime"
// 	"sort"
// 	"strconv"
// 	"strings"
// 	"sync"
// 	"sync/atomic"
// 	"syscall"
// 	"time"

// 	"gonum.org/v1/gonum/dsp/fourier"
// )

// //
// // ===================== CONFIG (tune for speed/quality) =====================
// //
// var (
// 	SampleRate     = 11025 // lower SR speeds FFT, 8k is also fine if you want more speed
// 	FFTSize        = 2048
// 	HopSize        = 512
// 	MinFreqHz      = 80
// 	MaxFreqHz      = 5000
// 	PeakAreaTime   = 8 // neighborhood in frames
// 	PeakAreaFreq   = 8 // neighborhood in bins
// 	PeakAbsThresh  = 6.0
// 	FanoutMinDelta = 1
// 	FanoutMaxDelta = 32 // shrink (e.g., 24) for more speed
// 	FanoutTargets  = 5  // 3–5 is a good balance
// 	Workers        = max(2, runtime.NumCPU()-1)
// 	MaxFrames      = 20000 // skip absurdly long/odd files
// 	DecodeSeconds  = 180   // decode at most N seconds for fingerprinting
// )

// //
// // ===================== TYPES =====================
// //
// type Posting struct {
// 	TrackID uint32
// 	Offset  uint32 // anchor frame offset in the track
// }

// type Index struct {
// 	H             map[uint64][]Posting // hash -> postings
// 	Tracks        []TrackMeta
// 	TrackIDByPath map[string]uint32
// }

// type TrackMeta struct {
// 	ID     uint32
// 	Path   string
// 	DurS   float64
// 	Title  string
// 	Artist string
// 	Album  string
// 	Track  string
// }

// //
// // ===================== FFT Provider (buffer-reuse, GPU-pluggable) =====================
// //
// type FFTProvider interface {
// 	New(n int) FFT
// }

// type FFT interface {
// 	// Compute power spectrum (0..N/2) into out and return it (no alloc).
// 	PowerReal(frame []float32, out []float64) []float64
// }

// type CPUFFTProvider struct{}

// type cpuFFT struct {
// 	r2c *fourier.FFT
// 	N   int
// 	buf []float64 // reusable real buffer for input
// }

// func (p *CPUFFTProvider) New(n int) FFT {
// 	return &cpuFFT{r2c: fourier.NewFFT(n), N: n, buf: make([]float64, n)}
// }

// func (f *cpuFFT) PowerReal(frame []float32, out []float64) []float64 {
// 	for i := 0; i < f.N; i++ {
// 		if i < len(frame) {
// 			f.buf[i] = float64(frame[i])
// 		} else {
// 			f.buf[i] = 0
// 		}
// 	}
// 	coeffs := f.r2c.Coefficients(nil, f.buf)
// 	N2 := f.N/2 + 1
// 	for k := 0; k < N2; k++ {
// 		re := real(coeffs[k])
// 		im := imag(coeffs[k])
// 		out[k] = re*re + im*im
// 	}
// 	return out
// }

// //
// // ===================== DECODING (ffmpeg) =====================
// //
// func decodeToMonoF32(ctx context.Context, path string) ([]float32, error) {
// 	cmd := exec.CommandContext(ctx, "ffmpeg",
// 		"-v", "error", "-hide_banner", "-nostats", "-nostdin",
// 		"-i", path,
// 		"-map", "a:0",
// 		"-t", fmt.Sprint(DecodeSeconds), // cap length for speed/robustness
// 		"-ac", "1",
// 		"-ar", fmt.Sprint(SampleRate),
// 		"-f", "f32le",
// 		"pipe:1",
// 	)
// 	// Ensure timeouts kill entire process group
// 	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

// 	var out bytes.Buffer
// 	cmd.Stdout = &out
// 	cmd.Stderr = os.Stderr
// 	if err := cmd.Run(); err != nil {
// 		return nil, fmt.Errorf("ffmpeg decode failed: %w", err)
// 	}
// 	b := out.Bytes()
// 	if len(b)%4 != 0 {
// 		return nil, errors.New("unexpected ffmpeg float32le length")
// 	}
// 	n := len(b) / 4
// 	samples := make([]float32, n)
// 	for i := 0; i < n; i++ {
// 		u := uint32(b[4*i]) | uint32(b[4*i+1])<<8 | uint32(b[4*i+2])<<16 | uint32(b[4*i+3])<<24
// 		samples[i] = math.Float32frombits(u)
// 	}
// 	return samples, nil
// }

// //
// // ===================== METADATA (ffprobe) =====================
// //
// type FFProbeFormat struct {
// 	Tags     map[string]string `json:"tags"`
// 	Duration string            `json:"duration"`
// }
// type FFProbeResult struct {
// 	Format FFProbeFormat `json:"format"`
// }

// func probeMetadata(ctx context.Context, path string) (title, artist, album, track string, durS float64, err error) {
// 	cmd := exec.CommandContext(ctx, "ffprobe",
// 		"-v", "error",
// 		"-show_entries", "format=duration:format_tags=title,artist,album,track",
// 		"-of", "json", path,
// 	)
// 	var out bytes.Buffer
// 	cmd.Stdout = &out
// 	cmd.Stderr = os.Stderr
// 	if err := cmd.Run(); err != nil {
// 		return "", "", "", "", 0, err
// 	}
// 	var r FFProbeResult
// 	if err := json.Unmarshal(out.Bytes(), &r); err != nil {
// 		return "", "", "", "", 0, err
// 	}
// 	d, _ := strconv.ParseFloat(r.Format.Duration, 64)
// 	return r.Format.Tags["title"], r.Format.Tags["artist"], r.Format.Tags["album"], r.Format.Tags["track"], d, nil
// }

// //
// // ===================== STREAMING STFT PEAK PICKER (fast + low memory) =====================
// //
// type Peak struct {
// 	T int     // frame index
// 	F int     // bin index
// 	A float64 // dBFS (relative to global max in band)
// }

// func hann(n int) []float32 {
// 	w := make([]float32, n)
// 	for i := 0; i < n; i++ {
// 		w[i] = float32(0.5 * (1 - math.Cos(2*math.Pi*float64(i)/float64(n))))
// 	}
// 	return w
// }
// func clamp(x, lo, hi int) int { if x < lo { return lo }; if x > hi { return hi }; return x }

// func stftPeaks(samples []float32, fftProv FFTProvider) []Peak {
// 	if len(samples) < FFTSize {
// 		return nil
// 	}
// 	frames := 1 + (len(samples)-FFTSize)/HopSize
// 	if frames > MaxFrames {
// 		return nil // guard against absurdly long/broken files
// 	}

// 	win := hann(FFTSize)
// 	fft := fftProv.New(FFTSize)
// 	N2 := FFTSize/2 + 1
// 	minBin := clamp(int(float64(MinFreqHz)/float64(SampleRate)*float64(FFTSize)), 1, N2-1)
// 	maxBin := clamp(int(float64(MaxFreqHz)/float64(SampleRate)*float64(FFTSize)), minBin+1, N2-1)

// 	// Ring of power spectra: only need a neighborhood of +/- PeakAreaTime
// 	ringW := 2*PeakAreaTime + 1
// 	ring := make([][]float64, ringW)
// 	for i := range ring {
// 		ring[i] = make([]float64, N2)
// 	}
// 	frameBuf := make([]float32, FFTSize)

// 	maxPow := 1e-12
// 	peaks := make([]Peak, 0, frames/2)

// 	for t := 0; t < frames; t++ {
// 		start := t * HopSize
// 		for i := 0; i < FFTSize; i++ {
// 			frameBuf[i] = samples[start+i] * win[i]
// 		}
// 		slot := ring[t%ringW]
// 		fft.PowerReal(frameBuf, slot)

// 		for k := minBin; k <= maxBin; k++ {
// 			if slot[k] > maxPow {
// 				maxPow = slot[k]
// 			}
// 		}

// 		// Only evaluate peaks when we have full neighborhood on both sides.
// 		// IMPORTANT: need at least 2*PeakAreaTime frames before to avoid negative indexing.
// 		if t >= 2*PeakAreaTime && t+PeakAreaTime < frames {
// 			center := t - PeakAreaTime
// 			cslot := ring[center%ringW]

// 			for k := minBin; k <= maxBin; k++ {
// 				v := cslot[k]
// 				if v < 1e-12 {
// 					v = 1e-12
// 				}
// 				db := 10 * math.Log10(v/maxPow)
// 				if db < -PeakAbsThresh {
// 					continue
// 				}
// 				// local-maximum test in neighborhood using raw power (faster)
// 				isMax := true
// 				for dt := -PeakAreaTime; dt <= PeakAreaTime && isMax; dt++ {
// 					ts := center + dt
// 					// extra guard (belt-and-suspenders)
// 					if ts < 0 || ts >= frames {
// 						continue
// 					}
// 					row := ring[ts%ringW]
// 					for df := -PeakAreaFreq; df <= PeakAreaFreq; df++ {
// 						kk := k + df
// 						if kk < 0 || kk >= N2 || (dt == 0 && df == 0) {
// 							continue
// 						}
// 						if row[kk] > cslot[k] {
// 							isMax = false
// 							break
// 						}
// 					}
// 				}
// 				if isMax {
// 					peaks = append(peaks, Peak{T: center, F: k, A: db})
// 				}
// 			}
// 		}
// 	}
// 	return peaks
// }

// //
// // ===================== HASHING =====================
// //
// func packHash(f1, f2, dt uint32) uint64 {
// 	return (uint64(f1&0xFFFF) << 48) | (uint64(f2&0xFFFF) << 32) | (uint64(dt&0xFFFF) << 16)
// }

// func buildHashes(peaks []Peak) map[uint64][]int {
// 	byTime := make(map[int][]Peak)
// 	for _, p := range peaks {
// 		byTime[p.T] = append(byTime[p.T], p)
// 	}
// 	ts := make([]int, 0, len(byTime))
// 	for t := range byTime {
// 		ts = append(ts, t)
// 	}
// 	sort.Ints(ts)

// 	res := make(map[uint64][]int)
// 	for _, t := range ts {
// 		anchors := byTime[t]
// 		for _, a := range anchors {
// 			// targets in [t+min, t+max]
// 			cands := make([]Peak, 0, 16)
// 			for dt := FanoutMinDelta; dt <= FanoutMaxDelta; dt++ {
// 				if ps, ok := byTime[t+dt]; ok {
// 					cands = append(cands, ps...)
// 				}
// 			}
// 			if len(cands) == 0 {
// 				continue
// 			}
// 			sort.Slice(cands, func(i, j int) bool { return cands[i].A > cands[j].A })
// 			limit := FanoutTargets
// 			if limit > len(cands) {
// 				limit = len(cands)
// 			}
// 			for i := 0; i < limit; i++ {
// 				b := cands[i]
// 				dt := b.T - a.T
// 				if dt <= 0 {
// 					continue
// 				}
// 				key := packHash(uint32(a.F), uint32(b.F), uint32(dt))
// 				res[key] = append(res[key], a.T)
// 			}
// 		}
// 	}
// 	return res
// }

// //
// // ===================== INDEXING (concurrent, robust) =====================
// //
// func indexFolder(ctx context.Context, folder string) (*Index, error) {
// 	idx := &Index{H: make(map[uint64][]Posting), TrackIDByPath: make(map[string]uint32)}
// 	paths := []string{}
// 	err := filepath.WalkDir(folder, func(path string, d os.DirEntry, err error) error {
// 		if err != nil {
// 			return err
// 		}
// 		if d.IsDir() {
// 			return nil
// 		}
// 		ext := strings.ToLower(filepath.Ext(d.Name()))
// 		if ext == ".m4a" || ext == ".mp4" || ext == ".aac" || ext == ".mp3" {
// 			paths = append(paths, path)
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		return nil, err
// 	}
// 	if len(paths) == 0 {
// 		return nil, fmt.Errorf("no audio files found in %s", folder)
// 	}

// 	fmt.Printf("Found %d files. Indexing with %d workers...\n", len(paths), Workers)

// 	var processed, skipped int64
// 	go func() {
// 		t := time.NewTicker(10 * time.Second)
// 		defer t.Stop()
// 		for range t.C {
// 			total := atomic.LoadInt64(&processed) + atomic.LoadInt64(&skipped)
// 			fmt.Printf("[WATCHDOG] %d/%d done (processed=%d skipped=%d)\n",
// 				total, len(paths), atomic.LoadInt64(&processed), atomic.LoadInt64(&skipped))
// 		}
// 	}()

// 	fftProv := &CPUFFTProvider{}

// 	type result struct {
// 		path string
// 		h    map[uint64][]int
// 		meta TrackMeta
// 		err  error
// 	}

// 	jobs := make(chan string, len(paths))
// 	out := make(chan result, len(paths))
// 	var wg sync.WaitGroup

// 	// Workers
// 	for w := 0; w < Workers; w++ {
// 		wg.Add(1)
// 		go func() {
// 			defer wg.Done()
// 			for p := range jobs {
// 				func() {
// 					defer func() {
// 						if r := recover(); r != nil {
// 							out <- result{path: p, err: fmt.Errorf("panic: %v", r)}
// 						}
// 					}()

// 					// Entire job bounded by 3 minutes
// 					jobCtx, jobCancel := context.WithTimeout(ctx, 3*time.Minute)
// 					defer jobCancel()

// 					// Decode (<=2m)
// 					ctxDec, cancelDec := context.WithTimeout(jobCtx, 2*time.Minute)
// 					samps, err := decodeToMonoF32(ctxDec, p)
// 					cancelDec()
// 					if err != nil {
// 						out <- result{path: p, err: err}
// 						return
// 					}

// 					// Probe (<=30s)
// 					ctxMeta, cancelMeta := context.WithTimeout(jobCtx, 30*time.Second)
// 					title, artist, album, track, du, metaErr := probeMetadata(ctxMeta, p)
// 					cancelMeta()
// 					if metaErr != nil {
// 						fmt.Printf("[WARN] probe failed for %s: %v\n", p, metaErr)
// 					}

// 					// Analysis watchdog (peaks + hashes) => <=30s
// 					done := make(chan struct{})
// 					var hashes map[uint64][]int
// 					go func() {
// 						peaks := stftPeaks(samps, fftProv)
// 						hashes = buildHashes(peaks)
// 						close(done)
// 					}()

// 					select {
// 					case <-done:
// 						out <- result{
// 							path: p,
// 							h:    hashes,
// 							meta: TrackMeta{Path: p, DurS: du, Title: title, Artist: artist, Album: album, Track: track},
// 						}
// 					case <-time.After(30 * time.Second):
// 						out <- result{path: p, err: fmt.Errorf("analysis timeout")}
// 					}
// 				}()
// 			}
// 		}()
// 	}

// 	// Enqueue
// 	for i, p := range paths {
// 		fmt.Printf("[ENQUEUE] %d/%d %s\n", i+1, len(paths), p)
// 		jobs <- p
// 	}
// 	close(jobs)

// 	// Collector
// 	go func() {
// 		wg.Wait()
// 		close(out)
// 	}()

// 	for r := range out {
// 		if r.err != nil {
// 			atomic.AddInt64(&skipped, 1)
// 			fmt.Printf("[ERROR] %s -> %v\n", r.path, r.err)
// 		} else {
// 			atomic.AddInt64(&processed, 1)
// 			id := uint32(len(idx.Tracks))
// 			meta := r.meta
// 			meta.ID = id
// 			idx.Tracks = append(idx.Tracks, meta)
// 			idx.TrackIDByPath[meta.Path] = id
// 			for key, offs := range r.h {
// 				ps := idx.H[key]
// 				for _, off := range offs {
// 					ps = append(ps, Posting{TrackID: id, Offset: uint32(off)})
// 				}
// 				idx.H[key] = ps
// 			}
// 		}
// 		total := atomic.LoadInt64(&processed) + atomic.LoadInt64(&skipped)
// 		fmt.Printf("[%d/%d] processed=%d skipped=%d\r",
// 			total, len(paths), atomic.LoadInt64(&processed), atomic.LoadInt64(&skipped))
// 	}
// 	fmt.Printf("\nIndexed %d tracks (%d skipped), %d unique hashes.\n",
// 		len(idx.Tracks), atomic.LoadInt64(&skipped), len(idx.H))
// 	return idx, nil
// }

// //
// // ===================== MATCHING =====================
// //
// type Match struct {
// 	TrackID uint32
// 	Track   TrackMeta
// 	Score   int
// 	Delta   int
// 	Conf    float64
// }

// func matchQuery(ctx context.Context, idx *Index, queryPath string) ([]Match, error) {
// 	samps, err := decodeToMonoF32(ctx, queryPath)
// 	if err != nil {
// 		return nil, err
// 	}
// 	fftProv := &CPUFFTProvider{}
// 	peaks := stftPeaks(samps, fftProv)
// 	qh := buildHashes(peaks)

// 	votes := make(map[uint32]map[int]int)
// 	totalQ := 0
// 	for key, qOffsets := range qh {
// 		totalQ += len(qOffsets)
// 		posts, ok := idx.H[key]
// 		if !ok {
// 			continue
// 		}
// 		for _, qoff := range qOffsets {
// 			for _, post := range posts {
// 				d := int(post.Offset) - qoff
// 				m := votes[post.TrackID]
// 				if m == nil {
// 					m = make(map[int]int)
// 					votes[post.TrackID] = m
// 				}
// 				m[d]++
// 			}
// 		}
// 	}

// 	matches := make([]Match, 0, len(votes))
// 	for tid, m := range votes {
// 		bestDelta, best := 0, 0
// 		for d, c := range m {
// 			if c > best {
// 				best, bestDelta = c, d
// 			}
// 		}
// 		meta := idx.Tracks[tid]
// 		conf := float64(best) / float64(max(1, totalQ))
// 		matches = append(matches, Match{TrackID: tid, Track: meta, Score: best, Delta: bestDelta, Conf: conf})
// 	}
// 	sort.Slice(matches, func(i, j int) bool {
// 		if matches[i].Score == matches[j].Score {
// 			return matches[i].Conf > matches[j].Conf
// 		}
// 		return matches[i].Score > matches[j].Score
// 	})
// 	return matches, nil
// }

// //
// // ===================== PERSISTENCE =====================
// //
// func saveIndex(path string, idx *Index) error {
// 	f, err := os.Create(path)
// 	if err != nil {
// 		return err
// 	}
// 	defer f.Close()
// 	bw := bufio.NewWriter(f)
// 	zw := gzip.NewWriter(bw)
// 	enc := gob.NewEncoder(zw)
// 	if err := enc.Encode(idx); err != nil {
// 		return err
// 	}
// 	if err := zw.Close(); err != nil {
// 		return err
// 	}
// 	return bw.Flush()
// }

// func loadIndex(path string) (*Index, error) {
// 	f, err := os.Open(path)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer f.Close()
// 	zr, err := gzip.NewReader(f)
// 	if err != nil {
// 		return nil, err
// 	}
// 	dec := gob.NewDecoder(zr)
// 	var idx Index
// 	if err := dec.Decode(&idx); err != nil {
// 		return nil, err
// 	}
// 	return &idx, nil
// }

// //
// // ===================== UTIL & MAIN =====================
// //
// func max(a, b int) int { if a > b { return a }; return b }
// func nextPow2(n int) int {
// 	if n <= 1 {
// 		return 1
// 	}
// 	return 1 << (bits.Len(uint(n-1)))
// }

// func main() {
// 	flag.Usage = func() {
// 		fmt.Fprintf(os.Stderr, "Shazam-style audio fingerprinting (Go)\n\n")
// 		fmt.Fprintf(os.Stderr, "Commands:\n")
// 		fmt.Fprintf(os.Stderr, "  index <folder> <index.gob.gz>\n")
// 		fmt.Fprintf(os.Stderr, "  query <index.gob.gz> <query.m4a>\n\n")
// 	}
// 	flag.Parse()
// 	if flag.NArg() < 1 {
// 		flag.Usage()
// 		os.Exit(2)
// 	}
// 	switch flag.Arg(0) {
// 	case "index":
// 		if flag.NArg() != 3 {
// 			flag.Usage()
// 			os.Exit(2)
// 		}
// 		folder := flag.Arg(1)
// 		out := flag.Arg(2)
// 		ctx := context.Background()
// 		start := time.Now()
// 		idx, err := indexFolder(ctx, folder)
// 		if err != nil {
// 			fmt.Fprintln(os.Stderr, "index error:", err)
// 			os.Exit(1)
// 		}
// 		if err := saveIndex(out, idx); err != nil {
// 			fmt.Fprintln(os.Stderr, "save error:", err)
// 			os.Exit(1)
// 		}
// 		fmt.Printf("Saved index to %s (tracks=%d, hashes=%d) in %v\n",
// 			out, len(idx.Tracks), len(idx.H), time.Since(start))

// 	case "query":
// 		if flag.NArg() != 3 {
// 			flag.Usage()
// 			os.Exit(2)
// 		}
// 		idxPath := flag.Arg(1)
// 		queryPath := flag.Arg(2)
// 		idx, err := loadIndex(idxPath)
// 		if err != nil {
// 			fmt.Fprintln(os.Stderr, "load error:", err)
// 			os.Exit(1)
// 		}
// 		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
// 		defer cancel()
// 		start := time.Now()
// 		matches, err := matchQuery(ctx, idx, queryPath)
// 		if err != nil {
// 			fmt.Fprintln(os.Stderr, "match error:", err)
// 			os.Exit(1)
// 		}
// 		fmt.Printf("Top matches for %s (took %v):\n", filepath.Base(queryPath), time.Since(start))
// 		for i, m := range matches {
// 			if i >= 10 {
// 				break
// 			}
// 			secOffset := float64(m.Delta*HopSize) / float64(SampleRate)
// 			fmt.Printf("%2d. score=%4d conf=%.3f delta=%.2fs  %s – %s (%s)\n",
// 				i+1, m.Score, m.Conf, secOffset, m.Track.Artist, m.Track.Title, m.Track.Album)
// 		}

// 	default:
// 		flag.Usage()
// 		os.Exit(2)
// 	}
// }