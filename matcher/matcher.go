// matcher.go
package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	xxhash "github.com/OneOfOne/xxhash"
	"github.com/dgraph-io/badger/v3"
	"github.com/dhowden/tag"
	"github.com/mjibson/go-dsp/fft"
)

const (
	IndexDirLocal = "/srv/indexdb"
	SampleRate    = 44100
	WindowSize    = 4096
	HopSize       = 2048
	PeakNeighborhood = 3
	MaxPeaksPerFrame = 10
	MaxTargets = 3
	QuantizeBins = 1024
	MaxTimeDeltaMs = 3000
	MaxCandidates = 10
)

// simplified peak struct same as indexer
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
		fmt.Println("usage: matcher <query.m4a>")
		os.Exit(2)
	}
	query := os.Args[1]
	db, err := badger.Open(badger.DefaultOptions(IndexDirLocal).WithLogger(nil))
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	samples, err := decodeToPCM(query)
	if err != nil {
		log.Fatalf("decode: %v", err)
	}
	spec := spectrogram(samples, WindowSize, HopSize)
	peaks := detectPeaks(spec)
	hashes := build4DHashes(peaks)

	// tally map: trackID -> (offsetMs -> count)
	type OffMap map[int64]int
	tally := map[string]OffMap{}
	var mu sync.Mutex

	// for each query hash, look up in DB
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for _, h := range hashes {
		wg.Add(1)
		sem <- struct{}{}
		go func(h Finger) {
			defer wg.Done()
			defer func() { <-sem }()
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, h.hash)
			err := db.View(func(txn *badger.Txn) error {
				item, err := txn.Get(key)
				if err != nil {
					if err == badger.ErrKeyNotFound {
						return nil
					}
					return err
				}
				return item.Value(func(val []byte) error {
					// val may contain many lines "trackID|anchorMs\n" appended; parse them
					sc := bufio.NewScanner(bytes.NewReader(val))
					for sc.Scan() {
						line := sc.Text()
						parts := strings.Split(line, "|")
						if len(parts) != 2 {
							continue
						}
						tid := parts[0]
						dbAnchorMs, _ := strconv.ParseInt(parts[1], 10, 64)
						offset := dbAnchorMs - h.anchorMs
						mu.Lock()
						if tally[tid] == nil {
							tally[tid] = OffMap{}
						}
						tally[tid][offset]++
						mu.Unlock()
					}
					return sc.Err()
				})
			})
			if err != nil {
				log.Printf("db view err: %v", err)
			}
		}(h)
	}
	wg.Wait()

	// convert to candidate list: compute best offset votes per track and total matches
	type Candidate struct {
		TrackID    string
		BestOffset int64
		Votes      int
		TotalHits  int
	}
	cands := []Candidate{}
	for tid, om := range tally {
		bestVotes := 0
		total := 0
		bestOffset := int64(0)
		for off, v := range om {
			total += v
			if v > bestVotes {
				bestVotes = v
				bestOffset = off
			}
		}
		cands = append(cands, Candidate{TrackID: tid, BestOffset: bestOffset, Votes: bestVotes, TotalHits: total})
	}
	// rank by Votes (the aligned votes), then total hits
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].Votes == cands[j].Votes {
			return cands[i].TotalHits > cands[j].TotalHits
		}
		return cands[i].Votes > cands[j].Votes
	})

	top := MaxCandidates
	if len(cands) < top {
		top = len(cands)
	}
	// print one line per top candidate: "trackID | votes aligned | totalHits | offsetMs | title - artist"
	for i := 0; i < top; i++ {
		c := cands[i]
		title, artist := readEmbeddedMetadata(c.TrackID)
		fmt.Printf("%s | alignedVotes=%d | totalHits=%d | offsetMs=%d | %s - %s\n",
			c.TrackID, c.Votes, c.TotalHits, c.BestOffset, title, artist)
	}
}

func decodeToPCM(path string) ([]float64, error) {
	cmd := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-i", path,
		"-f", "s16le", "-acodec", "pcm_s16le", "-ac", fmt.Sprintf("%d", 1), "-ar", fmt.Sprintf("%d", SampleRate), "-")
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

func readEmbeddedMetadata(trackID string) (string, string) {
	path := filepath.Join("./srv/tracks", trackID+".m4a")
	f, err := os.Open(path)
	if err != nil {
		return "unknown", "unknown"
	}
	defer f.Close()
	m, err := tag.ReadFrom(f)
	if err != nil {
		return "unknown", "unknown"
	}
	return m.Title(), m.Artist()
}