// indexer.go
package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	xxhash "github.com/OneOfOne/xxhash"
	"github.com/dgraph-io/badger/v3"
	"github.com/mjibson/go-dsp/fft"
)

const (
	TrackDir        = "/srv/tracks"
	IndexDir        = "/srv/indexdb"
	SampleRate      = 44100
	Channels        = 1
	WindowSize      = 4096    // FFT window size
	HopSize         = 2048    // hop between windows
	PeakNeighborhood = 3      // freq bins neighborhood to consider for local maxima
	MaxPeaksPerFrame = 10     // limit peaks per frame
	MaxTargets       = 3      // for each anchor pick up to 3 targets to make 4D hash
	MaxTimeDeltaMs   = 3000   // look forward up to 3s for pairing
	QuantizeBins     = 1024   // quantize frequencies into this many bins (for hashing)
	BatchSize        = 1000   // batch DB writes
)

// index entry format in value: trackID|anchorMs\n (text line). Cheap and portable.
func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	db, err := badger.Open(badger.DefaultOptions(IndexDir).WithLogger(nil))
	if err != nil {
		log.Fatalf("badger open: %v", err)
	}
	defer db.Close()

	paths, err := filepath.Glob(filepath.Join(TrackDir, "*.m4a"))
	if err != nil {
		log.Fatalf("glob: %v", err)
	}
	log.Printf("Found %d tracks\n", len(paths))

	fileCh := make(chan string, 256)
	var wg sync.WaitGroup
	workers := runtime.NumCPU()
	log.Printf("Spawning %d workers\n", workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range fileCh {
				if err := processOne(db, p); err != nil {
					log.Printf("err processing %s: %v", p, err)
				}
			}
		}()
	}

	for _, p := range paths {
		fileCh <- p
	}
	close(fileCh)
	wg.Wait()
	log.Println("Indexing complete.")
}

func processOne(db *badger.DB, path string) error {
	trackID := strings.TrimSuffix(filepath.Base(path), ".m4a")
	samples, err := decodeToPCM(path)
	if err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	spec := spectrogram(samples, WindowSize, HopSize)
	peaks := detectPeaks(spec)
	if len(peaks) == 0 {
		return errors.New("no peaks")
	}

	hashes := build4DHashes(peaks)
	if len(hashes) == 0 {
		return errors.New("no hashes")
	}

	// batch write to badger
	wb := db.NewWriteBatch()
	defer wb.Cancel()
	count := 0
	for _, h := range hashes {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, h.hash)
		val := fmt.Sprintf("%s|%d\n", trackID, h.anchorMs)
		if err := wb.Set(key, []byte(val)); err != nil {
			return err
		}
		count++
		if count%BatchSize == 0 {
			if err := wb.Flush(); err != nil {
				return err
			}
		}
	}
	if err := wb.Flush(); err != nil {
		return err
	}
	return nil
}

// decodeToPCM uses ffmpeg to output signed 16-bit little-endian PCM mono at SampleRate.
func decodeToPCM(path string) ([]float64, error) {
	cmd := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-i", path,
		"-f", "s16le", "-acodec", "pcm_s16le", "-ac", fmt.Sprintf("%d", Channels), "-ar", fmt.Sprintf("%d", SampleRate), "-")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	data := out.Bytes()
	// 16-bit signed little-endian -> float64 in [-1,1]
	samples := make([]float64, 0, len(data)/2)
	reader := bytes.NewReader(data)
	for {
		var v int16
		if err := binary.Read(reader, binary.LittleEndian, &v); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		samples = append(samples, float64(v)/32768.0)
	}
	return samples, nil
}

type Peak struct {
	TimeFrame int   // frame index
	FreqBin   int   // frequency bin index
	Mag       float64
	// time in ms = TimeFrame * HopSize / SampleRate * 1000
}

func spectrogram(samples []float64, window, hop int) [][]float64 {
	// Hanning window
	win := make([]float64, window)
	for i := 0; i < window; i++ {
		win[i] = 0.5 * (1 - math.Cos(2*math.Pi*float64(i)/float64(window-1)))
	}
	numFrames := int(math.Max(0, float64(len(samples)-window)/float64(hop))) + 1
	spec := make([][]float64, numFrames)
	for f := 0; f < numFrames; f++ {
		start := f * hop
		frame := make([]float64, window)
		for i := 0; i < window; i++ {
			frame[i] = samples[start+i] * win[i]
		}
		// convert to complex for FFT
		complexFrame := make([]complex128, window)
		for i := 0; i < window; i++ {
			complexFrame[i] = complex(frame[i], 0)
		}
		fftRes := fft.FFTReal(frame)
		mags := make([]float64, window/2)
		for i := 0; i < window/2; i++ {
			mags[i] = cmplxAbs(fftRes[i]) // magnitude
		}
		spec[f] = mags
	}
	return spec
}

func cmplxAbs(c complex128) float64 {
	return math.Sqrt(real(c)*real(c) + imag(c)*imag(c))
}

func detectPeaks(spec [][]float64) []Peak {
	peaks := []Peak{}
	numFrames := len(spec)
	if numFrames == 0 {
		return peaks
	}
	numBins := len(spec[0])
	for t := 0; t < numFrames; t++ {
		type candidate struct{ bin int; mag float64 }
		cands := make([]candidate, 0, 16)
		for b := 0; b < numBins; b++ {
			mag := spec[t][b]
			// local neighborhood
			isPeak := true
			for nb := b-PeakNeighborhood; nb <= b+PeakNeighborhood; nb++ {
				if nb < 0 || nb >= numBins || nb == b {
					continue
				}
				if spec[t][nb] >= mag {
					isPeak = false
					break
				}
			}
			if isPeak && mag > 1e-6 { // threshold
				cands = append(cands, candidate{bin: b, mag: mag})
			}
		}
		// keep top K peaks by magnitude in this frame
		if len(cands) > 0 {
			// simple partial sort
			if len(cands) > MaxPeaksPerFrame {
				// sort descending and trim
				for i := 0; i < MaxPeaksPerFrame; i++ {
					maxI := i
					for j := i + 1; j < len(cands); j++ {
						if cands[j].mag > cands[maxI].mag {
							maxI = j
						}
					}
					cands[i], cands[maxI] = cands[maxI], cands[i]
				}
				cands = cands[:MaxPeaksPerFrame]
			}
			for _, c := range cands {
				peaks = append(peaks, Peak{TimeFrame: t, FreqBin: c.bin, Mag: c.mag})
			}
		}
	}
	return peaks
}

// Represent a 4D hash: anchor freq, three target freqs, and time deltas quantized.
type Finger struct {
	hash     uint64
	anchorMs int64
}

func build4DHashes(peaks []Peak) []Finger {
	// Build time -> peaks mapping for quick lookups
	frameMap := map[int][]Peak{}
	for _, p := range peaks {
		frameMap[p.TimeFrame] = append(frameMap[p.TimeFrame], p)
	}
	// sort peak frames ascending is not required but helps
	var out []Finger
	// convert frame index -> ms
	frameToMs := func(frame int) int64 {
		return int64(frame*HopSize*1000/SampleRate)
	}
	// anchor: iterate peaks; for each anchor, find up to MaxTargets peaks in next MaxTimeDeltaMs
	for i, anchor := range peaks {
		anchorMs := frameToMs(anchor.TimeFrame)
		// collect candidate target peaks within time window
		targets := []Peak{}
		maxFrameDelta := (MaxTimeDeltaMs * SampleRate) / (HopSize * 1000)
		for j := i + 1; j < len(peaks) && (peaks[j].TimeFrame-anchor.TimeFrame) <= maxFrameDelta; j++ {
			if peaks[j].TimeFrame <= anchor.TimeFrame {
				continue
			}
			targets = append(targets, peaks[j])
		}
		// we need at least 3 targets to form a 4D hash
		if len(targets) < MaxTargets {
			continue
		}
		// choose up to MaxTargets deterministically: e.g., nearest in time or highest mag
		// we'll pick the first MaxTargets
		selected := targets[:MaxTargets]
		// quantize freq bins into buckets
		qf := func(bin int) uint16 {
			// bin ranges 0..WindowSize/2
			bins := WindowSize / 2
			val := int(math.Floor(float64(bin) * float64(QuantizeBins) / float64(bins)))
			if val < 0 {
				val = 0
			}
			if val >= QuantizeBins {
				val = QuantizeBins - 1
			}
			return uint16(val)
		}
		aq := qf(anchor.FreqBin)
		tq1 := qf(selected[0].FreqBin)
		tq2 := qf(selected[1].FreqBin)
		tq3 := qf(selected[2].FreqBin)
		// quantize time deltas (ms) into small ints (0..65535)
		dq := func(dt int64) uint16 {
			if dt < 0 {
				dt = 0
			}
			if dt > 65535 {
				return 65535
			}
			return uint16(dt)
		}
		d1 := dq(frameToMs(selected[0].TimeFrame) - anchorMs)
		d2 := dq(frameToMs(selected[1].TimeFrame) - anchorMs)
		d3 := dq(frameToMs(selected[2].TimeFrame) - anchorMs)
		// produce bytes for hashing: aq,tq1,tq2,tq3,d1,d2,d3
		buf := make([]byte, 2*7)
		offset := 0
		binary.BigEndian.PutUint16(buf[offset:], aq); offset += 2
		binary.BigEndian.PutUint16(buf[offset:], tq1); offset += 2
		binary.BigEndian.PutUint16(buf[offset:], tq2); offset += 2
		binary.BigEndian.PutUint16(buf[offset:], tq3); offset += 2
		binary.BigEndian.PutUint16(buf[offset:], d1); offset += 2
		binary.BigEndian.PutUint16(buf[offset:], d2); offset += 2
		binary.BigEndian.PutUint16(buf[offset:], d3); offset += 2
		// fast 64-bit hash
		h := xxhash.Checksum64(buf)
		out = append(out, Finger{hash: h, anchorMs: anchorMs})
	}
	return out
}