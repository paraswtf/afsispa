package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"math/cmplx"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"gonum.org/v1/gonum/dsp/fourier"
)

const (
	// DSP (speed + robustness)
	targetSampleRate   = 11025
	frameSize          = 2048
	hopSize            = 1024
	peakNeighborhoodTf = 2
	peakNeighborhoodF  = 15
	minPeakDB          = -55.0

	// Landmark pairing
	targetZoneMinDT   = 1
	targetZoneMaxDT   = 60
	maxPairsPerAnchor = 4
	topKPerFrame      = 5

	// Concurrency
	defaultWorkers = 0 // 0 = auto (NumCPU-1, min 2)
)

// ---------------- Types ----------------

type TrackMeta struct {
	ID       int
	Path     string
	Artist   string
	Album    string
	Title    string
	RelPath  string
	Duration float64
}

type Posting struct {
	TrackID int
	AnchorT int
}

type FingerprintDB struct {
	Index      map[uint64][]Posting
	Tracks     []TrackMeta
	SampleRate int
	FrameSize  int
	HopSize    int
	CreatedAt  time.Time
}

type Peak struct {
	T int
	F int
	M float64
}

type Hash struct {
	Hash uint64
	T1   int
}

type MatchCandidate struct {
	Meta          TrackMeta
	Votes         int
	TotalVotes    int
	OffsetFrames  int
	OffsetSeconds float64
	Confidence    float64 // 0..1 relative to best in this query set
}

// Segments & manifest (append-only indexing)
type SegmentInfo struct {
	Path      string    `json:"path"`
	CreatedAt time.Time `json:"created_at"`
	NumTracks int       `json:"num_tracks"`
	Params    struct {
		SampleRate int `json:"sr"`
		FrameSize  int `json:"frame"`
		HopSize    int `json:"hop"`
	} `json:"params"`
}

type Manifest struct {
	Segments   []SegmentInfo `json:"segments"`
	SampleRate int           `json:"sr"`
	FrameSize  int           `json:"frame"`
	HopSize    int           `json:"hop"`
}

// ---------------- Main ----------------

func main() {
	log.SetFlags(0)

	mode := flag.String("mode", "", "add | query | compact | (legacy: index)")
	dataset := flag.String("dataset", "", "folder with new songs to index (Artist/Album/*.m4a)")
	segmentOut := flag.String("segment", "", "segment output file (when mode=add); default seg-<timestamp>.gob")
	manifestPath := flag.String("manifest", "manifest.json", "segment manifest path (for add/query/compact)")
	indexFile := flag.String("index", "index.gob", "legacy single-index file (query fallback)")
	queryFile := flag.String("file", "", "query recording (.m4a)")
	topK := flag.Int("top", 10, "number of top matches to show")
	workers := flag.Int("workers", defaultWorkers, "concurrent workers for indexing (0=auto)")
	compactOut := flag.String("compact-into", "", "output segment path when mode=compact; default seg-merged-<timestamp>.gob")
	flag.Parse()

	switch *mode {
	case "add":
		if *dataset == "" {
			log.Fatal("missing -dataset")
		}
		db, err := buildIndex(*dataset, *workers)
		if err != nil {
			log.Fatalf("add error: %v", err)
		}
		seg := *segmentOut
		if seg == "" {
			seg = fmt.Sprintf("seg-%s.gob", time.Now().Format("20060102-150405"))
		}
		if err := saveDB(db, seg); err != nil {
			log.Fatalf("save segment error: %v", err)
		}
		if err := appendSegmentToManifest(*manifestPath, seg, db); err != nil {
			log.Fatalf("manifest update error: %v", err)
		}
		fmt.Printf("✅ Added segment %s (%d tracks). Manifest: %s\n", seg, len(db.Tracks), *manifestPath)

	case "query":
		if *queryFile == "" {
			log.Fatal("missing -file")
		}
		// Prefer manifest multi-segment if present; else fallback to legacy single index
		if fileExists(*manifestPath) {
			m, err := loadManifest(*manifestPath)
			if err != nil {
				log.Fatalf("load manifest error: %v", err)
			}
			results, err := matchAcrossSegmentsTopK(m, *queryFile, *topK)
			if err != nil {
				log.Fatalf("match error: %v", err)
			}
			printTop(results)
		} else {
			// legacy path
			db, err := loadDB(*indexFile)
			if err != nil {
				log.Fatalf("load index error: %v", err)
			}
			results, err := matchFileTopK(db, *queryFile, *topK)
			if err != nil {
				log.Fatalf("match error: %v", err)
			}
			printTop(results)
		}

	case "compact":
		// Merge all segments into a single big segment; update manifest to only that segment
		if !fileExists(*manifestPath) {
			log.Fatalf("manifest not found: %s", *manifestPath)
		}
		m, err := loadManifest(*manifestPath)
		if err != nil {
			log.Fatalf("load manifest error: %v", err)
		}
		if len(m.Segments) == 0 {
			log.Fatal("nothing to compact: manifest has zero segments")
		}
		out := *compactOut
		if out == "" {
			out = fmt.Sprintf("seg-merged-%s.gob", time.Now().Format("20060102-150405"))
		}
		merged, err := mergeSegments(m.Segments)
		if err != nil {
			log.Fatalf("compact error: %v", err)
		}
		if err := saveDB(merged, out); err != nil {
			log.Fatalf("save merged segment error: %v", err)
		}
		// rewrite manifest to only the merged segment
		newM := &Manifest{
			SampleRate: merged.SampleRate,
			FrameSize:  merged.FrameSize,
			HopSize:    merged.HopSize,
			Segments:   []SegmentInfo{},
		}
		si := SegmentInfo{
			Path:      out,
			CreatedAt: time.Now(),
			NumTracks: len(merged.Tracks),
		}
		si.Params.SampleRate = merged.SampleRate
		si.Params.FrameSize = merged.FrameSize
		si.Params.HopSize = merged.HopSize
		newM.Segments = append(newM.Segments, si)
		if err := saveManifest(*manifestPath, newM); err != nil {
			log.Fatalf("write manifest error: %v", err)
		}
		fmt.Printf("🧹 Compacted %d segments → %s (%d tracks). Manifest updated.\n", len(m.Segments), out, len(merged.Tracks))

	case "index":
		// Legacy monolithic index (kept for compatibility)
		if *dataset == "" {
			log.Fatal("missing -dataset")
		}
		db, err := buildIndex(*dataset, *workers)
		if err != nil {
			log.Fatalf("index error: %v", err)
		}
		if err := saveDB(db, *indexFile); err != nil {
			log.Fatalf("save error: %v", err)
		}
		fmt.Printf("✅ Indexed %d tracks → %s\n", len(db.Tracks), *indexFile)

	default:
		fmt.Println("Usage:")
		fmt.Println("  Add (append-only segment):")
		fmt.Println("    go run . -mode add -dataset /path/to/folder -manifest manifest.json [-segment seg-001.gob] [-workers N]")
		fmt.Println("  Query (uses manifest if present):")
		fmt.Println("    go run . -mode query -manifest manifest.json -file query.m4a [-top 10]")
		fmt.Println("  Compact (merge all segments into one):")
		fmt.Println("    go run . -mode compact -manifest manifest.json [-compact-into seg-merged.gob]")
		fmt.Println("  Legacy single-file index:")
		fmt.Println("    go run . -mode index -dataset /path/to/folder -out index.gob")
	}
}

// ---------------- Manifest helpers ----------------

func loadManifest(path string) (*Manifest, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func saveManifest(path string, m *Manifest) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0644)
}

func appendSegmentToManifest(manifestPath string, segPath string, db *FingerprintDB) error {
	var m *Manifest
	if fileExists(manifestPath) {
		existing, err := loadManifest(manifestPath)
		if err != nil {
			return err
		}
		// sanity on params (must match)
		if existing.SampleRate != 0 &&
			(existing.SampleRate != db.SampleRate || existing.FrameSize != db.FrameSize || existing.HopSize != db.HopSize) {
			return fmt.Errorf("segment params mismatch: manifest(%d,%d,%d) vs segment(%d,%d,%d)",
				existing.SampleRate, existing.FrameSize, existing.HopSize,
				db.SampleRate, db.FrameSize, db.HopSize)
		}
		m = existing
	} else {
		m = &Manifest{
			SampleRate: db.SampleRate,
			FrameSize:  db.FrameSize,
			HopSize:    db.HopSize,
			Segments:   []SegmentInfo{},
		}
	}
	si := SegmentInfo{
		Path:      segPath,
		CreatedAt: time.Now(),
		NumTracks: len(db.Tracks),
	}
	si.Params.SampleRate = db.SampleRate
	si.Params.FrameSize = db.FrameSize
	si.Params.HopSize = db.HopSize

	m.Segments = append(m.Segments, si)
	return saveManifest(manifestPath, m)
}

// ---------------- Indexing (parallel, with ETA) ----------------

func buildIndex(root string, workerArg int) (*FingerprintDB, error) {
	tracks, err := collectTracks(root)
	if err != nil {
		return nil, err
	}
	if len(tracks) == 0 {
		return nil, fmt.Errorf("no .m4a files under %s", root)
	}

	db := &FingerprintDB{
		Index:      make(map[uint64][]Posting, 1<<20),
		Tracks:     make([]TrackMeta, 0, len(tracks)),
		SampleRate: targetSampleRate,
		FrameSize:  frameSize,
		HopSize:    hopSize,
		CreatedAt:  time.Now(),
	}

	// progress bar
	p := mpb.New(mpb.WithWidth(64))
	bar := p.AddBar(int64(len(tracks)),
		mpb.PrependDecorators(
			decor.Name("Indexing: "),
			decor.CountersNoUnit("%d / %d"),
		),
		mpb.AppendDecorators(
			decor.Percentage(),
			decor.EwmaETA(decor.ET_STYLE_GO, 60),
		),
	)

	// concurrency
	w := workerArg
	if w <= 0 {
		w = runtime.NumCPU() - 1
		if w < 2 {
			w = 2
		}
	}
	type result struct {
		meta   TrackMeta
		hashes []Hash
		err    error
	}

	jobs := make(chan string, len(tracks))
	results := make(chan result, len(tracks))

	var wg sync.WaitGroup
	for i := 0; i < w; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			win := hann(frameSize)
			fft := fourier.NewFFT(frameSize)
			for path := range jobs {
				samples, sr, dur, err := decodeToMonoFloat32(path, targetSampleRate)
				if err != nil || sr != targetSampleRate {
					results <- result{err: err}
					continue
				}
				spec := stftWithPlan(samples, frameSize, hopSize, win, fft)
				peaks := findSpectralPeaksTopK(spec, minPeakDB, peakNeighborhoodTf, peakNeighborhoodF, topKPerFrame)
				hashes := makeLandmarkHashesFast(peaks, targetZoneMinDT, targetZoneMaxDT, maxPairsPerAnchor)

				meta := parseMetaFromPath(root, path)
				meta.Path = path
				meta.RelPath = relPath(root, path)
				meta.Duration = dur

				results <- result{meta: meta, hashes: hashes, err: nil}
			}
		}()
	}

	for _, t := range tracks {
		jobs <- t
	}
	close(jobs)

	id := 0
	go func() {
		wg.Wait()
		close(results)
	}()

	for r := range results {
		bar.Increment()
		if r.err != nil {
			continue
		}
		r.meta.ID = id
		db.Tracks = append(db.Tracks, r.meta)
		for _, h := range r.hashes {
			db.Index[h.Hash] = append(db.Index[h.Hash], Posting{TrackID: r.meta.ID, AnchorT: h.T1})
		}
		id++
	}

	p.Wait()
	return db, nil
}

func collectTracks(root string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.ToLower(filepath.Ext(path)) == ".m4a" {
			out = append(out, path)
		}
		return nil
	})
	return out, err
}

func relPath(root, p string) string {
	r, err := filepath.Rel(root, p)
	if err != nil {
		return p
	}
	return r
}

func parseMetaFromPath(root, p string) TrackMeta {
	r := relPath(root, p)
	parts := splitClean(r)
	artist, album, title := "Unknown", "Unknown", filepath.Base(p)
	if len(parts) >= 3 {
		artist = parts[len(parts)-3]
		album = parts[len(parts)-2]
		title = strings.TrimSuffix(parts[len(parts)-1], filepath.Ext(parts[len(parts)-1]))
	}
	return TrackMeta{Artist: artist, Album: album, Title: title, RelPath: r}
}

func splitClean(path string) []string {
	parts := []string{}
	for _, x := range strings.Split(path, string(os.PathSeparator)) {
		if x != "" {
			parts = append(parts, x)
		}
	}
	return parts
}

// ---------------- Matching ----------------

func matchAcrossSegmentsTopK(m *Manifest, queryFile string, k int) ([]MatchCandidate, error) {
	if len(m.Segments) == 0 {
		return nil, fmt.Errorf("manifest has no segments")
	}
	type aggKey struct{ artist, album, title string }
	agg := map[aggKey]MatchCandidate{}

	for _, seg := range m.Segments {
		db, err := loadDB(seg.Path)
		if err != nil {
			continue // skip missing/bad segment
		}
		segResults, err := matchFileTopK(db, queryFile, max(50, k)) // get more, merge later
		if err != nil {
			continue
		}
		for _, r := range segResults {
			key := aggKey{r.Meta.Artist, r.Meta.Album, r.Meta.Title}
			prev, ok := agg[key]
			if !ok {
				agg[key] = r
			} else {
				combined := prev
				combined.Votes += r.Votes
				combined.TotalVotes += r.TotalVotes
				// keep offset from the stronger one
				if r.Votes > prev.Votes {
					combined.OffsetFrames = r.OffsetFrames
					combined.OffsetSeconds = r.OffsetSeconds
				}
				agg[key] = combined
			}
		}
	}

	if len(agg) == 0 {
		return nil, nil
	}

	finals := make([]MatchCandidate, 0, len(agg))
	bestVotes := 0
	for _, v := range agg {
		if v.Votes > bestVotes {
			bestVotes = v.Votes
		}
	}
	if bestVotes == 0 {
		bestVotes = 1
	}
	for _, v := range agg {
		v.Confidence = float64(v.Votes) / float64(bestVotes)
		finals = append(finals, v)
	}

	sort.Slice(finals, func(i, j int) bool {
		if finals[i].Votes != finals[j].Votes {
			return finals[i].Votes > finals[j].Votes
		}
		if finals[i].TotalVotes != finals[j].TotalVotes {
			return finals[i].TotalVotes > finals[j].TotalVotes
		}
		if abs(finals[i].OffsetFrames) != abs(finals[j].OffsetFrames) {
			return abs(finals[i].OffsetFrames) < abs(finals[j].OffsetFrames)
		}
		ai := finals[i].Meta.Artist + "/" + finals[i].Meta.Album + "/" + finals[i].Meta.Title
		aj := finals[j].Meta.Artist + "/" + finals[j].Meta.Album + "/" + finals[j].Meta.Title
		return ai < aj
	})

	if k <= 0 || k > len(finals) {
		k = len(finals)
	}
	return finals[:k], nil
}

func matchFileTopK(db *FingerprintDB, file string, k int) ([]MatchCandidate, error) {
	samples, sr, _, err := decodeToMonoFloat32(file, db.SampleRate)
	if err != nil {
		return nil, err
	}
	if sr != db.SampleRate {
		return nil, fmt.Errorf("sample rate mismatch: expected %d got %d", db.SampleRate, sr)
	}
	spec := stft(samples, db.FrameSize, db.HopSize)
	peaks := findSpectralPeaks(spec, minPeakDB, peakNeighborhoodTf, peakNeighborhoodF)
	hashes := makeLandmarkHashes(peaks, targetZoneMinDT, targetZoneMaxDT, maxPairsPerAnchor)
	if len(hashes) == 0 {
		return nil, nil
	}

	type key struct{ trackID, offset int }
	vote := make(map[key]int)
	trackTotalVotes := make(map[int]int)

	for _, h := range hashes {
		postings, ok := db.Index[h.Hash]
		if !ok {
			continue
		}
		for _, p := range postings {
			offset := p.AnchorT - h.T1
			vote[key{p.TrackID, offset}]++
			trackTotalVotes[p.TrackID]++
		}
	}
	if len(vote) == 0 {
		return nil, nil
	}

	type raw struct{ trackID, offset, votes, total int }
	raws := make([]raw, 0, len(vote))
	for k2, v2 := range vote {
		raws = append(raws, raw{
			trackID: k2.trackID,
			offset:  k2.offset,
			votes:   v2,
			total:   trackTotalVotes[k2.trackID],
		})
	}

	sort.Slice(raws, func(i, j int) bool {
		if raws[i].votes != raws[j].votes {
			return raws[i].votes > raws[j].votes
		}
		if raws[i].total != raws[j].total {
			return raws[i].total > raws[j].total
		}
		ai, aj := abs(raws[i].offset), abs(raws[j].offset)
		if ai != aj {
			return ai < aj
		}
		return raws[i].trackID < raws[j].trackID
	})

	// best offset per track
	bestByTrack := make(map[int]raw)
	for _, r := range raws {
		if _, ok := bestByTrack[r.trackID]; !ok {
			bestByTrack[r.trackID] = r
		}
	}

	finals := make([]MatchCandidate, 0, len(bestByTrack))
	bestVotes := 0
	for _, r := range bestByTrack {
		if r.votes > bestVotes {
			bestVotes = r.votes
		}
	}
	if bestVotes == 0 {
		bestVotes = 1
	}

	for _, r := range bestByTrack {
		m := db.Tracks[r.trackID]
		finals = append(finals, MatchCandidate{
			Meta:          m,
			Votes:         r.votes,
			TotalVotes:    r.total,
			OffsetFrames:  r.offset,
			OffsetSeconds: float64(r.offset*db.HopSize) / float64(db.SampleRate),
			Confidence:    float64(r.votes) / float64(bestVotes),
		})
	}

	sort.Slice(finals, func(i, j int) bool {
		if finals[i].Votes != finals[j].Votes {
			return finals[i].Votes > finals[j].Votes
		}
		if finals[i].TotalVotes != finals[j].TotalVotes {
			return finals[i].TotalVotes > finals[j].TotalVotes
		}
		if abs(finals[i].OffsetFrames) != abs(finals[j].OffsetFrames) {
			return abs(finals[i].OffsetFrames) < abs(finals[j].OffsetFrames)
		}
		return finals[i].Meta.ID < finals[j].Meta.ID
	})

	if k <= 0 || k > len(finals) {
		k = len(finals)
	}
	return finals[:k], nil
}

func printTop(results []MatchCandidate) {
	if len(results) == 0 {
		fmt.Println("❌ No matches found.")
		return
	}
	fmt.Printf("Top %d candidates:\n", len(results))
	for i, r := range results {
		fmt.Printf("%2d) %s / %s / %s  | votes=%d  total=%d  offset=%.2fs  confidence=%.2f\n",
			i+1, r.Meta.Artist, r.Meta.Album, r.Meta.Title,
			r.Votes, r.TotalVotes, r.OffsetSeconds, r.Confidence)
	}
}

// ---------------- Compaction (merge segments, no rehash) ----------------

func mergeSegments(segments []SegmentInfo) (*FingerprintDB, error) {
	merged := &FingerprintDB{
		Index:      make(map[uint64][]Posting, 1<<22),
		Tracks:     []TrackMeta{},
		SampleRate: targetSampleRate,
		FrameSize:  frameSize,
		HopSize:    hopSize,
		CreatedAt:  time.Now(),
	}
	nextID := 0

	for _, si := range segments {
		db, err := loadDB(si.Path)
		if err != nil {
			continue // skip missing/bad
		}
		// sanity params (must match)
		if db.SampleRate != merged.SampleRate || db.FrameSize != merged.FrameSize || db.HopSize != merged.HopSize {
			return nil, fmt.Errorf("segment %s has different params (got %d/%d/%d, want %d/%d/%d)",
				si.Path, db.SampleRate, db.FrameSize, db.HopSize, merged.SampleRate, merged.FrameSize, merged.HopSize)
		}
		idMap := make(map[int]int, len(db.Tracks))
		for _, t := range db.Tracks {
			newID := nextID
			nextID++
			idMap[t.ID] = newID
			t.ID = newID
			merged.Tracks = append(merged.Tracks, t)
		}
		for h, postings := range db.Index {
			for _, p := range postings {
				merged.Index[h] = append(merged.Index[h], Posting{TrackID: idMap[p.TrackID], AnchorT: p.AnchorT})
			}
		}
	}
	return merged, nil
}

// ---------------- Fingerprinting ----------------

func stft(x []float32, n, hop int) [][]complex128 {
	win := hann(n)
	fft := fourier.NewFFT(n)
	return stftWithPlan(x, n, hop, win, fft)
}

func stftWithPlan(x []float32, n, hop int, win []float64, fft *fourier.FFT) [][]complex128 {
	if n <= 0 || hop <= 0 {
		panic("bad stft params")
	}
	frames := 1 + int(math.Max(0, float64(len(x)-n))/float64(hop))
	spec := make([][]complex128, frames)
	buf := make([]float64, n)
	for i := 0; i < frames; i++ {
		start := i * hop
		for k := 0; k < n; k++ {
			if start+k < len(x) {
				buf[k] = float64(x[start+k]) * win[k]
			} else {
				buf[k] = 0
			}
		}
		out := fft.Coefficients(nil, buf)
		spec[i] = out[:n/2] // positive freqs
	}
	return spec
}

func hann(n int) []float64 {
	w := make([]float64, n)
	for i := range w {
		w[i] = 0.5 * (1 - math.Cos(2*math.Pi*float64(i)/float64(n-1)))
	}
	return w
}

func findSpectralPeaks(spec [][]complex128, minDB float64, neighT, neighF int) []Peak {
	peaks := make([]Peak, 0, len(spec)*4)
	mags := make([][]float64, len(spec))
	for t := range spec {
		row := spec[t]
		m := make([]float64, len(row))
		for f := range row {
			m[f] = 20*math.Log10(cmplx.Abs(row[f]) + 1e-12)
		}
		mags[t] = m
	}
	for t := 0; t < len(mags); t++ {
		for f := 1; f < len(mags[t])-1; f++ {
			v := mags[t][f]
			if v < minDB {
				continue
			}
			ok := true
			for dt := -neighT; dt <= neighT && ok; dt++ {
				tt := t + dt
				if tt < 0 || tt >= len(mags) {
					continue
				}
				start := max(0, f-neighF)
				end := min(len(mags[tt])-1, f+neighF)
				for ff := start; ff <= end; ff++ {
					if dt == 0 && ff == f {
						continue
					}
					if mags[tt][ff] > v {
						ok = false
						break
					}
				}
			}
			if ok {
				peaks = append(peaks, Peak{T: t, F: f, M: v})
			}
		}
	}
	return peaks
}

func findSpectralPeaksTopK(spec [][]complex128, minDB float64, neighT, neighF, topK int) []Peak {
	mags := make([][]float64, len(spec))
	for t := range spec {
		row := spec[t]
		m := make([]float64, len(row))
		for f := range row {
			m[f] = 20*math.Log10(cmplx.Abs(row[f]) + 1e-12)
		}
		mags[t] = m
	}
	peaks := make([]Peak, 0, len(spec)*topK)
	for t := 0; t < len(mags); t++ {
		framePeaks := frameLocalMaxima(t, mags, minDB, neighT, neighF)
		if len(framePeaks) > topK {
			sort.Slice(framePeaks, func(i, j int) bool { return framePeaks[i].M > framePeaks[j].M })
			framePeaks = framePeaks[:topK]
		}
		peaks = append(peaks, framePeaks...)
	}
	return peaks
}

func frameLocalMaxima(t int, mags [][]float64, minDB float64, neighT, neighF int) []Peak {
	out := make([]Peak, 0, 8)
	mt := mags[t]
	for f := 1; f < len(mt)-1; f++ {
		v := mt[f]
		if v < minDB {
			continue
		}
		ok := true
		for dt := -neighT; dt <= neighT && ok; dt++ {
			tt := t + dt
			if tt < 0 || tt >= len(mags) {
				continue
			}
			row := mags[tt]
			start := max(0, f-neighF)
			end := min(len(row)-1, f+neighF)
			for ff := start; ff <= end; ff++ {
				if dt == 0 && ff == f {
					continue
				}
				if row[ff] > v {
					ok = false
					break
				}
			}
		}
		if ok {
			out = append(out, Peak{T: t, F: f, M: v})
		}
	}
	return out
}

func makeLandmarkHashes(peaks []Peak, minDT, maxDT, maxPairs int) []Hash {
	byT := map[int][]Peak{}
	minT, maxT := math.MaxInt32, -1
	for _, p := range peaks {
		byT[p.T] = append(byT[p.T], p)
		if p.T < minT {
			minT = p.T
		}
		if p.T > maxT {
			maxT = p.T
		}
	}
	hashes := make([]Hash, 0, len(peaks)*2)
	for t := minT; t <= maxT; t++ {
		anchors := byT[t]
		if len(anchors) == 0 {
			continue
		}
		made := 0
		for _, a := range anchors {
			for dt := minDT; dt <= maxDT && t+dt <= maxT; dt++ {
				cands := byT[t+dt]
				for _, b := range cands {
					hashes = append(hashes, Hash{Hash: packHash(a.F, b.F, dt), T1: t})
					made++
					if made >= maxPairs {
						break
					}
				}
				if made >= maxPairs {
					break
				}
			}
			made = 0
		}
	}
	return hashes
}

func makeLandmarkHashesFast(peaks []Peak, minDT, maxDT, maxPairs int) []Hash {
	byT := map[int][]Peak{}
	minT, maxT := math.MaxInt32, -1
	for _, p := range peaks {
		byT[p.T] = append(byT[p.T], p)
		if p.T < minT {
			minT = p.T
		}
		if p.T > maxT {
			maxT = p.T
		}
	}
	for t := range byT {
		sort.Slice(byT[t], func(i, j int) bool { return byT[t][i].M > byT[t][j].M })
	}

	hashes := make([]Hash, 0, len(peaks)*2)
	for t := minT; t <= maxT; t++ {
		anchors := byT[t]
		if len(anchors) == 0 {
			continue
		}
		anchorLimit := min(len(anchors), topKPerFrame)
		for ai := 0; ai < anchorLimit; ai++ {
			a := anchors[ai]
			made := 0
			for dt := minDT; dt <= maxDT && t+dt <= maxT; dt++ {
				cands := byT[t+dt]
				if len(cands) == 0 {
					continue
				}
				partnerLimit := min(len(cands), topKPerFrame)
				for bi := 0; bi < partnerLimit; bi++ {
					b := cands[bi]
					hashes = append(hashes, Hash{Hash: packHash(a.F, b.F, dt), T1: t})
					made++
					if made >= maxPairs {
						break
					}
				}
				if made >= maxPairs {
					break
				}
			}
		}
	}
	return hashes
}

func packHash(f1, f2, dt int) uint64 {
	// [f1:22][f2:22][dt:20]
	return (uint64(uint32(f1)) << 42) | (uint64(uint32(f2)) << 20) | uint64(uint32(dt))
}

// ---------------- Audio Decode ----------------

func decodeToMonoFloat32(path string, sampleRate int) ([]float32, int, float64, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, 0, 0, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dur, _ := probeDuration(path)

	args := []string{
		"-hide_banner", "-v", "error",
		"-i", path,
		"-ac", "1",
		"-ar", fmt.Sprintf("%d", sampleRate),
		"-f", "f32le",
		"pipe:1",
	}
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &bytes.Buffer{}

	if err := cmd.Run(); err != nil {
		return nil, 0, 0, fmt.Errorf("ffmpeg decode: %w", err)
	}
	raw := out.Bytes()
	if len(raw)%4 != 0 {
		return nil, 0, 0, errors.New("unexpected byte length")
	}
	n := len(raw) / 4
	samples := make([]float32, n)
	br := bytes.NewReader(raw)
	buf := bufio.NewReader(br)
	for i := 0; i < n; i++ {
		var b [4]byte
		if _, err := buf.Read(b[:]); err != nil {
			return nil, 0, 0, err
		}
		u := uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		samples[i] = math.Float32frombits(u)
	}
	return samples, sampleRate, dur, nil
}

func probeDuration(path string) (float64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ffprobe",
		"-v", "error",
		"-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1",
		path)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &bytes.Buffer{}
	if err := cmd.Run(); err != nil {
		return 0, err
	}
	s := strings.TrimSpace(out.String())
	if s == "" {
		return 0, errors.New("no duration")
	}
	s = strings.ReplaceAll(s, ",", ".")
	val, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	return val, nil
}

// ---------------- DB ----------------

func saveDB(db *FingerprintDB, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := gob.NewEncoder(f)
	return enc.Encode(db)
}

func loadDB(path string) (*FingerprintDB, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var db FingerprintDB
	dec := gob.NewDecoder(f)
	if err := dec.Decode(&db); err != nil {
		return nil, err
	}
	// sane defaults
	if db.HopSize == 0 {
		db.HopSize = hopSize
	}
	if db.FrameSize == 0 {
		db.FrameSize = frameSize
	}
	if db.SampleRate == 0 {
		db.SampleRate = targetSampleRate
	}
	return &db, nil
}

// ---------------- Utils ----------------

func fileExists(p string) bool { _, err := os.Stat(p); return err == nil }
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}