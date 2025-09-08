// indexer.go
package main

import (
	"bytes"
	"context"
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
	"time"

	xxhash "github.com/OneOfOne/xxhash"
	"github.com/dgraph-io/badger/v3"
	"github.com/mjibson/go-dsp/fft"
)

const (
	TrackDir         = "/srv/tracks"
	IndexDir         = "/srv/indexdb"
	SampleRate       = 44100
	Channels         = 1
	WindowSize       = 4096
	HopSize          = 2048
	PeakNeighborhood = 3
	MaxPeaksPerFrame = 10
	MaxTargets       = 3
	MaxTimeDeltaMs   = 3000
	QuantizeBins     = 1024
	// DB writer batching
	DBChanBuf        = 1 << 16 // channel buffer for entries (adjust to memory)
	FlushEveryWrites = 10000   // flush writebatch every N writes
	FlushEveryMs     = 2000    // or every X milliseconds
	MinFileSizeBytes = 10 * 1024
)

type Peak struct {
	TimeFrame int
	FreqBin   int
	Mag       float64
}

type Finger struct {
	hash     uint64
	anchorMs int64
}

type KV struct {
	Key []byte
	Val []byte
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	db, err := badger.Open(badger.DefaultOptions(IndexDir).WithLogger(nil))
	if err != nil {
		log.Fatalf("badger open: %v", err)
	}
	defer db.Close()

	// find files
	paths, err := filepath.Glob(filepath.Join(TrackDir, "*.m4a"))
	if err != nil {
		log.Fatalf("glob tracks: %v", err)
	}
	log.Printf("Found %d tracks\n", len(paths))

	// channel for db writes
	dbCh := make(chan KV, DBChanBuf)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// start single db writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		dbWriter(ctx, db, dbCh)
	}()

	// start worker pool
	workers := runtime.NumCPU()
	log.Printf("Spawning %d workers\n", workers)
	fileCh := make(chan string, 512)
	var wwg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wwg.Add(1)
		go func() {
			defer wwg.Done()
			for p := range fileCh {
				if err := processOneAndEmit(p, dbCh); err != nil {
					log.Printf("err processing %s: %v", p, err)
				}
			}
		}()
	}

	// enqueue files
	for _, p := range paths {
		fileCh <- p
	}
	close(fileCh)
	wwg.Wait()

	// workers done; close db channel and wait for db writer to finish
	close(dbCh)
	// wait for dbWriter to flush and exit
	wg.Wait()
	cancel()
	log.Println("Indexing complete.")
}

// dbWriter runs in single goroutine, owns one WriteBatch and flushes periodically.
func dbWriter(ctx context.Context, db *badger.DB, ch <-chan KV) {
	wb := db.NewWriteBatch()
	defer wb.Cancel()
	writeCount := 0
	flushTicker := time.NewTicker(time.Millisecond * time.Duration(FlushEveryMs))
	defer flushTicker.Stop()

	flush := func() {
		if writeCount == 0 {
			return
		}
		if err := wb.Flush(); err != nil {
			log.Printf("wb.Flush error: %v", err)
			// try to recover: cancel and create a new batch
			wb.Cancel()
			wb = db.NewWriteBatch()
		}
		writeCount = 0
	}

	for {
		select {
		case kv, ok := <-ch:
			if !ok {
				flush()
				return
			}
			if err := wb.Set(kv.Key, kv.Val); err != nil {
				log.Printf("WriteBatch.Set error: %v; attempting flush and retry", err)
				flush()
				// create a fresh set attempt
				if err2 := wb.Set(kv.Key, kv.Val); err2 != nil {
					log.Printf("retry failed: %v", err2)
				} else {
					writeCount++
				}
			} else {
				writeCount++
			}
			if writeCount >= FlushEveryWrites {
				flush()
				// create new writebatch after flush
				wb.Cancel()
				wb = db.NewWriteBatch()
			}
		case <-flushTicker.C:
			flush()
			// reset batch if needed
			wb.Cancel()
			wb = db.NewWriteBatch()
		case <-ctx.Done():
			flush()
			return
		}
	}
}

// processOneAndEmit fingerprints a single file and emits KV pairs to dbCh
func processOneAndEmit(path string, dbCh chan<- KV) error {
	base := filepath.Base(path)
	if strings.HasPrefix(base, "._") || strings.HasPrefix(base, ".") {
		// skip mac resource files / hidden files
		log.Printf("skipping hidden/resource file: %s", path)
		return nil
	}
	fi, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}
	if fi.Size() < MinFileSizeBytes {
		// move to quarantine for manual inspection
		if err := moveToQuarantine(path, base); err != nil {
			log.Printf("move to quarantine failed: %v", err)
		} else {
			log.Printf("moved tiny file %s to quarantine", path)
		}
		return nil
	}

	// quick ffmpeg probe to ensure file is readable
	if err := ffprobe(path); err != nil {
		// move to quarantine
		if err2 := moveToQuarantine(path, base); err2 != nil {
			return fmt.Errorf("probe failed and move failed: %v; probeErr: %w", err2, err)
		}
		log.Printf("moved corrupt/unreadable file %s to quarantine", path)
		return nil
	}

	// decode to pcm
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

	trackID := strings.TrimSuffix(base, ".m4a")
	// emit kv pairs
	for _, h := range hashes {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, h.hash)
		val := fmt.Sprintf("%s|%d\n", trackID, h.anchorMs)
		dbCh <- KV{Key: key, Val: []byte(val)}
	}
	return nil
}

func moveToQuarantine(path, base string) error {
	qdir := "/srv/track-quarantine"
	if err := os.MkdirAll(qdir, 0755); err != nil {
		return err
	}
	dst := filepath.Join(qdir, base)
	return os.Rename(path, dst)
}

func ffprobe(path string) error {
	// run ffmpeg quick probe (null output) to check readability
	cmd := exec.Command("ffmpeg", "-v", "error", "-i", path, "-f", "null", "-")
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Run(); err != nil {
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
		fftRes := fft.FFTReal(frame)
		mags := make([]float64, window/2)
		for i := 0; i < window/2; i++ {
			mags[i] = cmplxAbs(fftRes[i])
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
			// local neighborhood check
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
			if isPeak && mag > 1e-6 {
				cands = append(cands, candidate{bin: b, mag: mag})
			}
		}
		if len(cands) > 0 {
			if len(cands) > MaxPeaksPerFrame {
				// partial selection: top K
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

func build4DHashes(peaks []Peak) []Finger {
	if len(peaks) == 0 {
		return nil
	}
	frameToMs := func(frame int) int64 {
		return int64(frame*HopSize*1000/SampleRate)
	}
	out := []Finger{}
	// iterate anchors; choose next MaxTargets peaks within time window
	for i, anchor := range peaks {
		anchorMs := frameToMs(anchor.TimeFrame)
		targets := []Peak{}
		maxFrameDelta := (MaxTimeDeltaMs * SampleRate) / (HopSize * 1000)
		for j := i + 1; j < len(peaks) && (peaks[j].TimeFrame-anchor.TimeFrame) <= maxFrameDelta; j++ {
			if peaks[j].TimeFrame <= anchor.TimeFrame {
				continue
			}
			targets = append(targets, peaks[j])
		}
		if len(targets) < MaxTargets {
			continue
		}
		selected := targets[:MaxTargets]
		qf := func(bin int) uint16 {
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
		buf := make([]byte, 14)
		offset := 0
		binary.BigEndian.PutUint16(buf[offset:], aq)
		offset += 2
		binary.BigEndian.PutUint16(buf[offset:], tq1)
		offset += 2
		binary.BigEndian.PutUint16(buf[offset:], tq2)
		offset += 2
		binary.BigEndian.PutUint16(buf[offset:], tq3)
		offset += 2
		binary.BigEndian.PutUint16(buf[offset:], d1)
		offset += 2
		binary.BigEndian.PutUint16(buf[offset:], d2)
		offset += 2
		binary.BigEndian.PutUint16(buf[offset:], d3)
		offset += 2
		h := xxhash.Checksum64(buf)
		out = append(out, Finger{hash: h, anchorMs: anchorMs})
	}
	return out
}