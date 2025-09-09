// fingerprint_debug.go
// Usage: go run fingerprint_debug.go /path/to/query.m4a
package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"

	xxhash "github.com/OneOfOne/xxhash"
	"github.com/mjibson/go-dsp/fft"
)

const (
	SampleRate        = 44100
	WindowSize        = 4096
	HopSize           = 2048
	PeakNeighborhood  = 3
	MaxPeaksPerFrame  = 10
	MaxTargets        = 3
	MaxTimeDeltaMs    = 3000
	QuantizeBins      = 1024
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

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: go run fingerprint_debug.go /path/to/query.m4a")
		os.Exit(2)
	}
	path := os.Args[1]
	abs, _ := filepath.Abs(path)
	fmt.Println("Query file:", abs)

	samples, err := decodeToPCM(path)
	if err != nil {
		log.Fatalf("decodeToPCM error: %v", err)
	}
	fmt.Printf("samples: %d (duration ~ %.2fs)\n", len(samples), float64(len(samples))/SampleRate)

	spec := spectrogram(samples, WindowSize, HopSize)
	fmt.Printf("frames: %d window=%d hop=%d\n", len(spec), WindowSize, HopSize)

	peaks := detectPeaks(spec)
	fmt.Printf("total peaks found: %d\n", len(peaks))

	hashes := build4DHashes(peaks)
	fmt.Printf("total hashes produced: %d\n", len(hashes))
	limit := 20
	if len(hashes) < limit {
		limit = len(hashes)
	}
	fmt.Printf("first %d hashes (hex) and anchorMs:\n", limit)
	for i := 0; i < limit; i++ {
		fmt.Printf("%3d: %016x  anchorMs=%d\n", i+1, hashes[i].hash, hashes[i].anchorMs)
	}
}

func decodeToPCM(path string) ([]float64, error) {
	cmd := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-i", path,
		"-f", "s16le", "-acodec", "pcm_s16le", "-ac", "1", "-ar", strconv.Itoa(SampleRate), "-")
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
	win := make([]float64, window)
	for i := 0; i < window; i++ {
		win[i] = 0.5 * (1 - math.Cos(2*math.Pi*float64(i)/float64(window-1)))
	}
	if len(samples) < window {
		return [][]float64{}
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
	if len(spec) == 0 {
		return peaks
	}
	numBins := len(spec[0])
	for t := 0; t < len(spec); t++ {
		type cand struct{ bin int; mag float64 }
		cands := make([]cand, 0, 16)
		for b := 0; b < numBins; b++ {
			mag := spec[t][b]
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
				cands = append(cands, cand{bin: b, mag: mag})
			}
		}
		if len(cands) > 0 {
			if len(cands) > MaxPeaksPerFrame {
				// partial selection of top K
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
		binary.BigEndian.PutUint16(buf[offset:], aq); offset += 2
		binary.BigEndian.PutUint16(buf[offset:], tq1); offset += 2
		binary.BigEndian.PutUint16(buf[offset:], tq2); offset += 2
		binary.BigEndian.PutUint16(buf[offset:], tq3); offset += 2
		binary.BigEndian.PutUint16(buf[offset:], d1); offset += 2
		binary.BigEndian.PutUint16(buf[offset:], d2); offset += 2
		binary.BigEndian.PutUint16(buf[offset:], d3); offset += 2
		h := xxhash.Checksum64(buf)
		out = append(out, Finger{hash: h, anchorMs: anchorMs})
	}
	return out
}