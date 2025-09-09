// matcher.go (patched)
// Usage: ./matcher /path/to/query.m4a
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	xxhash "github.com/OneOfOne/xxhash"
	"github.com/dgraph-io/badger/v3"
	"github.com/dhowden/tag"
	"github.com/mjibson/go-dsp/fft"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	IndexDirLocal     = "/srv/indexdb"
	SampleRate        = 44100
	WindowSize        = 4096
	HopSize           = 2048
	PeakNeighborhood  = 3
	MaxPeaksPerFrame  = 10
	MaxTargets        = 3
	QuantizeBins      = 1024
	MaxTimeDeltaMs    = 3000
	MaxCandidates     = 10
	MinAlignedVotes   = 3    // you can tweak this
	MinConfidence     = 0.01 // alignedVotes / totalQueryHashes threshold
	TrackFilesDir     = "/srv/tracks" // where your track files live
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
		fmt.Println("usage: matcher <query.m4a>")
		os.Exit(2)
	}
	query := os.Args[1]

	// open badger
	db, err := badger.Open(badger.DefaultOptions(IndexDirLocal).WithLogger(nil))
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	// try to init mongo client if MONGO_URI provided
	mongoClient, mongoDB := initMongoIfConfigured()
	if mongoClient != nil {
		defer func() {
			_ = mongoClient.Disconnect(context.Background())
		}()
	}

	// decode query
	samples, err := decodeToPCM(query)
	if err != nil {
		log.Fatalf("decode: %v", err)
	}
	spec := spectrogram(samples, WindowSize, HopSize)
	peaks := detectPeaks(spec)
	hashes := build4DHashes(peaks)
	if len(hashes) == 0 {
		log.Fatalf("no hashes generated from query")
	}
	totalQueryHashes := len(hashes)

	// tally: trackID -> map[offset]count
	type OffMap map[int64]int
	tally := map[string]OffMap{}
	var mu sync.Mutex
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

	// score candidates
	type Candidate struct {
		TrackID    string
		BestOffset int64
		Votes      int
		TotalHits  int
		Confidence float64 // Votes / totalQueryHashes
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
		conf := 0.0
		if totalQueryHashes > 0 {
			conf = float64(bestVotes) / float64(totalQueryHashes)
		}
		// apply light thresholds to avoid spamming output; you can tune
		if bestVotes >= MinAlignedVotes && conf >= MinConfidence {
			cands = append(cands, Candidate{
				TrackID:    tid,
				BestOffset: bestOffset,
				Votes:      bestVotes,
				TotalHits:  total,
				Confidence: conf,
			})
		}
	}
	if len(cands) == 0 {
		log.Println("No candidates passed thresholds. Try lowering thresholds or using a longer/cleaner query clip.")
		return
	}

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
	for i := 0; i < top; i++ {
		c := cands[i]
		title, artist := readMetadataOrMongo(c.TrackID, mongoDB)
		fmt.Printf("%s | alignedVotes=%d | totalHits=%d | offsetMs=%d | confidence=%.3f | %s - %s\n",
			c.TrackID, c.Votes, c.TotalHits, c.BestOffset, c.Confidence, title, artist)
	}
}

// readMetadataOrMongo: try embedded tags first; if missing, try mongo DB (if available)
func readMetadataOrMongo(trackID string, db *mongo.Database) (string, string) {
	title, artist := readEmbeddedMetadata(trackID)
	if title != "unknown" || artist != "unknown" {
		return title, artist
	}
	if db == nil {
		return "unknown", "unknown"
	}
	// try to find track document in a few likely collection names
	collNames := []string{"Track", "track", "tracks", "Tracks"}
	for _, cn := range collNames {
		coll := db.Collection(cn)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		var doc bson.M
		err := coll.FindOne(ctx, bson.M{"_id": trackID}).Decode(&doc)
		cancel()
		if err != nil {
			continue
		}
		// try fields
		if v, ok := doc["name"]; ok && v != nil {
			title = fmt.Sprint(v)
		} else if v, ok := doc["title"]; ok && v != nil {
			title = fmt.Sprint(v)
		}
		// simple attempt for artist: doc may have "artist" or via album relations
		if v, ok := doc["artist"]; ok && v != nil {
			artist = fmt.Sprint(v)
			return title, artist
		}
		// if albumId present, try to fetch album->artist via relations
		if aidI, ok := doc["albumId"]; ok && aidI != nil {
			aid := fmt.Sprint(aidI)
			// find album
			albumCollNames := []string{"Album", "album", "albums", "Albums"}
			var albumDoc bson.M
			for _, acn := range albumCollNames {
				ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
				err2 := db.Collection(acn).FindOne(ctx2, bson.M{"_id": aid}).Decode(&albumDoc)
				cancel2()
				if err2 == nil {
					break
				}
			}
			// if albumDoc found, search AlbumArtistRelations for artistId
			if albumDoc != nil {
				aarColls := []string{"AlbumArtistRelations", "albumartistrelations", "album_artist_relations", "albumArtistRelations", "AlbumArtistRelations"}
				for _, aar := range aarColls {
					ctx3, cancel3 := context.WithTimeout(context.Background(), 3*time.Second)
					var rel bson.M
					err3 := db.Collection(aar).FindOne(ctx3, bson.M{"albumId": aid}).Decode(&rel)
					cancel3()
					if err3 == nil {
						if atidI, ok := rel["artistId"]; ok && atidI != nil {
							artistId := fmt.Sprint(atidI)
							// query artist
							artistColls := []string{"Artist", "artist", "artists", "Artists"}
							for _, ac := range artistColls {
								ctx4, cancel4 := context.WithTimeout(context.Background(), 3*time.Second)
								var art bson.M
								err4 := db.Collection(ac).FindOne(ctx4, bson.M{"_id": artistId}).Decode(&art)
								cancel4()
								if err4 == nil {
									if v, ok := art["name"]; ok && v != nil {
										artist = fmt.Sprint(v)
										return title, artist
									}
								}
							}
						}
					}
				}
			}
		}
		// return whatever we found from track doc (even if artist empty)
		return title, artist
	}
	return "unknown", "unknown"
}

// readEmbeddedMetadata tries to read metadata tags from the file /srv/tracks/<trackID>.m4a
func readEmbeddedMetadata(trackID string) (string, string) {
	path := filepath.Join(TrackFilesDir, trackID+".m4a")
	f, err := os.Open(path)
	if err != nil {
		return "unknown", "unknown"
	}
	defer f.Close()
	m, err := tag.ReadFrom(f)
	if err != nil {
		return "unknown", "unknown"
	}
	t := m.Title()
	a := m.Artist()
	if strings.TrimSpace(t) == "" {
		t = "unknown"
	}
	if strings.TrimSpace(a) == "" {
		a = "unknown"
	}
	return t, a
}

// initMongoIfConfigured inspects MONGO_URI and attempts to connect.
// It returns (client, db) if successful, otherwise (nil, nil).
func initMongoIfConfigured() (*mongo.Client, *mongo.Database) {
	uri := os.Getenv("MONGO_URI")
	if strings.TrimSpace(uri) == "" {
		return nil, nil
	}
	// determine DB name: prefer MONGO_DB, otherwise parse URI path
	dbName := os.Getenv("MONGO_DB")
	if dbName == "" {
		u, err := url.Parse(uri)
		if err == nil {
			dbName = strings.Trim(u.Path, "/")
		}
	}
	if dbName == "" {
		log.Printf("MONGO_URI provided but DB name not found. Set MONGO_DB env var if automatic parse fails.")
		// continue: use empty name -> fallback not good; return nil
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clientOpts := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		log.Printf("mongo connect error: %v", err)
		return nil, nil
	}
	if err := client.Ping(ctx, nil); err != nil {
		log.Printf("mongo ping error: %v", err)
		_ = client.Disconnect(ctx)
		return nil, nil
	}
	return client, client.Database(dbName)
}

// --- Below: same audio fingerprinting helpers as before (spectrogram, peaks, hashing, decode) ---

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
			if err.Error() == "EOF" || err == io.EOF {
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