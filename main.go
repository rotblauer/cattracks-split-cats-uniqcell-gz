package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sync"
	"time"

	"github.com/golang/geo/s2"
	"github.com/hashicorp/golang-lru/v2"
	"github.com/tidwall/gjson"
	bolt "go.etcd.io/bbolt"
)

// // https://s2geometry.io/resources/s2cell_statistics.html
// // 23 	0.73 	1.52 	1.21 	m2 	  	83 cm 	116 cm 	  	110 cm 	120 cm 	422T
var sufficientPrecisionLevel = 23
var flagCellLevel = flag.Int("cell-level", sufficientPrecisionLevel, fmt.Sprintf("S2 Cell Level (Default=%d)", sufficientPrecisionLevel))
var flagDBPath = flag.String("db", filepath.Join(".", "cat-uniqs.db"), "Path to database for unique cat/tracks")
var flagCacheSize = flag.Int("cache-size", 100000, "Size of cache for unique cat/tracks")
var flagOutputRootFilepath = flag.String("output", filepath.Join(".", "output"), "Output root dir")
var flagWorkers = flag.Int("workers", 8, "Number of workers")
var compressionLevel = gzip.DefaultCompression
var flagCompression = flag.Int("compression-level", compressionLevel, fmt.Sprintf("Compression level (Default=%d, BestSpeed=%d, BestCompression=%d)", gzip.DefaultCompression, gzip.BestSpeed, gzip.BestCompression))

var db *bolt.DB
var cache *lru.Cache[string, bool]

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
}

func readLinesBatching(raw io.Reader, closeBatch func(lines [][]byte, last, next []byte) bool, workers int) (chan [][]byte, chan error, error) {
	bufferedContents := bufio.NewReaderSize(raw, 4096) // default 4096

	ch := make(chan [][]byte, workers)
	errs := make(chan error)

	go func(ch chan [][]byte, errs chan error, contents *bufio.Reader) {
		defer func(ch chan [][]byte, errs chan error) {
			// close(ch)
			// close(errs)
		}(ch, errs)

		last := []byte{}
		lines := [][]byte{}
		for {
			next, err := contents.ReadBytes('\n')
			if err != nil {
				// Send remaining lines, an error expected when EOF.
				if len(lines) > 0 {
					ch <- lines
					lines = [][]byte{}
				}
				errs <- err
				if err != io.EOF {
					return
				}
			} else {
				if closeBatch(lines, last, next) {
					ch <- lines
					lines = [][]byte{}
				}
				lines = append(lines, next)
			}
			last = next
		}
	}(ch, errs, bufferedContents)

	return ch, errs, nil
}

// file locks are used to prevent multiple processes from writing to the same file, which could damage the gzip file
var metaflock = new(sync.Mutex)
var flocks = map[string]*sync.Mutex{}

func appendGZipLines(lines [][]byte, fname string) error {
	if len(lines) == 0 {
		return nil
	}

	// flock the file
	metaflock.Lock()
	if _, ok := flocks[fname]; !ok {
		flocks[fname] = new(sync.Mutex)
	}
	metaflock.Unlock()
	flocks[fname].Lock()
	defer flocks[fname].Unlock()

	// gzip and append the lines to the year_month file
	//
	fi, err := os.OpenFile(fname, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer fi.Close()

	gzW, _ := gzip.NewWriterLevel(fi, compressionLevel)
	defer gzW.Close() // will happen before fi.Close()
	defer gzW.Flush() // will happen before gzW.Close()

	for i, line := range lines {
		_, err := gzW.Write(line)
		if err != nil {
			return err
		}
		if i%60*60*24 == 0 {
			gzW.Flush() // flush ~daily
		}
	}
	return nil
}

var aliases = map[*regexp.Regexp]string{
	regexp.MustCompile(`(?i)(Big.*P.*|Isaac.*|.*moto.*|iha)`): "ia",
	regexp.MustCompile(`(?i)(Big.*Ma.*)`):                     "jr",
	regexp.MustCompile("(?i)(Rye.*|Kitty.*)"):                 "rye",
	regexp.MustCompile("(?i)Kayleigh.*"):                      "kd",
	regexp.MustCompile("(?i)(KK.*|kek)"):                      "kk",
	regexp.MustCompile("(?i)Bob.*"):                           "rj",
	regexp.MustCompile("(?i)(Pam.*|Rathbone.*)"):              "pr",
	regexp.MustCompile("(?i)Ric"):                             "ric",
	regexp.MustCompile("(?i)Twenty7.*"):                       "mat",
}

func aliasOrName(name string) string {
	for r, a := range aliases {
		if r.MatchString(name) {
			return a
		}
	}
	return name
}

func mustGetCatName(line string) string {
	value := gjson.Get(line, "properties.Name").String()
	return aliasOrName(value)
}

func getCatTrackCacheKey(cat string, cellID s2.CellID) string {
	return fmt.Sprintf("%s:%s", cat, cellID.ToToken())
}

func trackCache(cat string, cellID s2.CellID) (uniq bool) {
	key := getCatTrackCacheKey(cat, cellID)
	if _, ok := cache.Get(key); ok {
		return false
	}
	cache.Add(key, true)
	return true
}

func trackDB(cat string, cellID s2.CellID) (uniq bool) {
	// check db
	var exists bool
	_ = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(cat))
		if b == nil {
			return nil
		}
		v := b.Get([]byte(cellID.ToToken()))
		exists = v != nil
		return nil
	})
	if exists {
		return false
	}
	_ = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(cat))
		if err != nil {
			return err
		}
		err = b.Put([]byte(cellID.ToToken()), []byte{0x1})
		if err != nil {
			return err
		}
		return nil
	})

	return true
}

func round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}

func toFixed(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return float64(round(num*output)) / output
}

func cellIDWithLevel(cellID s2.CellID, level int) s2.CellID {
	var lsb uint64 = 1 << (2 * (30 - level))
	truncatedCellID := (uint64(cellID) & -lsb) | lsb
	return s2.CellID(truncatedCellID)
}

func getTrackCellID(line string) s2.CellID {
	var coords []float64
	gjson.Get(line, "geometry.coordinates").ForEach(func(key, value gjson.Result) bool {
		// https://en.wikipedia.org/wiki/Decimal_degrees?useskin=vector#Precision
		// 6 => "individual humans"
		coords = append(coords, toFixed(value.Float(), 6))
		return true
	})
	return cellIDWithLevel(s2.CellIDFromLatLng(s2.LatLngFromDegrees(coords[1], coords[0])), *flagCellLevel)
}

func isCatTrackUniq(cat, trackLine string) bool {
	cellid := getTrackCellID(trackLine)
	if !trackCache(cat, cellid) {
		return false
	}
	if !trackDB(cat, cellid) {
		return false
	}
	return true
}

func main() {
	flag.Parse()
	compressionLevel = *flagCompression

	var err error
	db, err = bolt.Open(*flagDBPath, 0600, nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	cache, _ = lru.New[string, bool](*flagCacheSize)

	// ensure output dir exists
	_ = os.MkdirAll(*flagOutputRootFilepath, 0755)

	closeBatchName := func(lines [][]byte, last, next []byte) bool {
		ll := len(lines)
		if last == nil || ll == 0 {
			return false
		}
		_name := mustGetCatName(string(last))
		name := mustGetCatName(string(next))
		return _name != name
	}

	// workersWG is used for clean up processing after the reader has finished.
	workersWG := new(sync.WaitGroup)
	type work struct {
		name  string
		lines [][]byte
	}
	workCh := make(chan work, *flagWorkers)
	for i := 0; i < *flagWorkers; i++ {
		workerI := i
		go func() {
			for w := range workCh {
				defer workersWG.Done()
				start := time.Now()

				fname := filepath.Join(*flagOutputRootFilepath, fmt.Sprintf("%s.json.gz", w.name))
				log.Printf("[worker=%02d] Deduping %s lines=%d...\n", workerI+1, w.name, len(w.lines))

				uniqLines := [][]byte{}
				for _, line := range w.lines {
					if isCatTrackUniq(w.name, string(line)) {
						uniqLines = append(uniqLines, line)
					}
				}

				log.Printf("[worker=%02d] Deduped lines=%d uniq=%d took=%v, gzipping to %s...\n", workerI+1, len(w.lines), len(uniqLines), time.Since(start), fname)

				if err := appendGZipLines(uniqLines, fname); err != nil {
					log.Fatalln(err)
				}
				log.Printf("[worker=%02d] Wrote %d unique lines to %s, took %s\n", workerI+1, len(uniqLines), fname, time.Since(start))
			}
		}()
	}

	handleLinesBatch := func(lines [][]byte) {
		name := mustGetCatName(string(lines[0]))
		log.Printf("Handle batch %s GOROUTINES %d LINES %d", name, runtime.NumGoroutine(), len(lines))
		workersWG.Add(1)
		workCh <- work{name: name, lines: lines} //
	}

	linesCh, errCh, err := readLinesBatching(os.Stdin, closeBatchName, *flagWorkers)
	if err != nil {
		log.Fatalln(err)
	}

readLoop:
	for {
		select {
		case lines := <-linesCh:
			handleLinesBatch(lines) //
		case err := <-errCh:
			if err == io.EOF {
				log.Println("EOF")
				break readLoop
			}
			log.Fatal(err)
		}
	}
	// flush remaining lines
	for len(linesCh) > 0 {
		handleLinesBatch(<-linesCh)
	}
	close(linesCh)
	close(errCh)
	log.Println("Waiting for workers to finish...")
	workersWG.Wait()
	close(workCh)
}
