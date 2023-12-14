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
var compressionLevel = gzip.DefaultCompression
var defaultOutputRoot = filepath.Join(".", "output")

var flagCellLevel = flag.Int("cell-level", sufficientPrecisionLevel, fmt.Sprintf("S2 Cell Level (Default=%d)", sufficientPrecisionLevel))
var flagCompression = flag.Int("compression-level", compressionLevel, fmt.Sprintf("Compression level (Default=%d, BestSpeed=%d, BestCompression=%d)", gzip.DefaultCompression, gzip.BestSpeed, gzip.BestCompression))
var flagCacheSize = flag.Int("cache-size", 1_000_000, "Size of cache for unique cat/tracks")
var flagWorkers = flag.Int("workers", 8, "Number of workers")
var flagOutputRootFilepath = flag.String("output", defaultOutputRoot, "Output root dir")
var flagDBRootPath = flag.String("db", filepath.Join(defaultOutputRoot, "dbs"), "Path to databases root for unique cat/tracks")

var dbs = make(map[string]*bolt.DB)
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

func trackCache(cat string, cellID s2.CellID) (exists bool) {
	key := getCatTrackCacheKey(cat, cellID)
	if _, ok := cache.Get(key); ok {
		return ok
	}
	cache.Add(key, true)
	return false
}

var metaDBsLock = new(sync.Mutex)
var dbsMuLock = make(map[string]*sync.Mutex)

func getOrInitCatDB(cat string) *bolt.DB {
	if _, ok := dbsMuLock[cat]; !ok {
		metaDBsLock.Lock()
		dbsMuLock[cat] = new(sync.Mutex)
		metaDBsLock.Unlock()
	}
	dbsMuLock[cat].Lock()
	defer dbsMuLock[cat].Unlock()

	if db, ok := dbs[cat]; ok {
		return db
	}
	db, err := bolt.Open(filepath.Join(*flagDBRootPath, fmt.Sprintf("%s-level.%d.db", cat, *flagCellLevel)), 0600, nil)
	if err != nil {
		log.Fatalln(err)
	}
	dbs[cat] = db
	return db
}

func tracksDBExists(cat string, cellID s2.CellID) (err error, exists bool) {
	db := getOrInitCatDB(cat)
	// defer db.Close()
	// check db
	bucket := []byte{byte(*flagCellLevel)}
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		v := b.Get([]byte(cellID.ToToken()))
		exists = v != nil
		return nil
	})
	if err != nil {
		return err, false
	}
	if exists {
		return nil, true
	}
	return err, false
}

func tracksDBWriteUniqs(cat string, cellIDs []s2.CellID) (err error) {
	db := getOrInitCatDB(cat)
	// defer db.Close()
	return db.Update(func(tx *bolt.Tx) error {
		bucket := []byte{byte(*flagCellLevel)}
		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		for _, cellID := range cellIDs {
			err = b.Put([]byte(cellID.ToToken()), []byte{0x1})
			if err != nil {
				return err
			}
		}
		return nil
	})
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

func isCatTrackUniq(cat, trackLine string) (cellID s2.CellID, uniq bool) {
	cellID = getTrackCellID(trackLine)
	if trackCache(cat, cellID) {
		uniq = false
		return
	}
	if err, exists := tracksDBExists(cat, cellID); err != nil {
		log.Fatalln(err)
	} else if exists {
		uniq = false
		return
	}
	uniq = true
	return
}

func getTrackTime(line []byte) time.Time {
	return time.Time(gjson.GetBytes(line, "properties.Time").Time())
}

func main() {
	flag.Parse()
	compressionLevel = *flagCompression

	cache, _ = lru.New[string, bool](*flagCacheSize)
	defer func() {
		for cat, db := range dbs {
			log.Printf("Closing DB %s\n", cat)
			db.Close()
		}
	}()

	// ensure output dir exists
	_ = os.MkdirAll(*flagOutputRootFilepath, 0755)
	_ = os.MkdirAll(*flagDBRootPath, 0755)

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
		workerI := i + 1
		go func() {
			workerI := workerI
			for w := range workCh {
				defer workersWG.Done()
				start := time.Now()

				fname := filepath.Join(*flagOutputRootFilepath, fmt.Sprintf("%s.json.gz", w.name))
				log.Printf("[worker=%02d] Deduping %s lines=%d...\n", workerI, w.name, len(w.lines))

				uniqLines := [][]byte{}
				uniqCellIDs := []s2.CellID{}
				for _, line := range w.lines {
					cellID, uniq := isCatTrackUniq(w.name, string(line))
					if uniq {
						uniqLines = append(uniqLines, line)
						uniqCellIDs = append(uniqCellIDs, cellID)
					}
				}
				log.Printf("[worker=%02d] Deduped %s lines=%d uniq=%d took=%v\n", workerI, w.name, len(w.lines), len(uniqLines), time.Since(start))

				// noop if 0 unique lines
				if len(uniqLines) == 0 {
					continue
				}

				if err := tracksDBWriteUniqs(w.name, uniqCellIDs); err != nil {
					log.Fatalln(err)
				}

				if err := appendGZipLines(uniqLines, fname); err != nil {
					log.Fatalln(err)
				}
				log.Printf("[worker=%02d] Wrote %d unique lines to %s, took %s\n", workerI, len(uniqLines), fname, time.Since(start))
			}
		}()
	}

	// these only used for pretty logging, to see track time progress
	latestTrackTimePrefix := time.Time{}
	lastTimeLogPrefixUpdate := time.Now()

	handleLinesBatch := func(lines [][]byte) {
		name := mustGetCatName(string(lines[0]))

		// update latest track time progress if stale
		// don't do for every because it's unnecessarily expensive
		if latestTrackTimePrefix.IsZero() || time.Since(lastTimeLogPrefixUpdate) > time.Second*10 {
			latestTrackTimePrefix = getTrackTime(lines[0])
			lastTimeLogPrefixUpdate = time.Now()
		}
		log.Printf("@ %s | Handle batch %s GOROUTINES %d LINES %d", latestTrackTimePrefix.Format("2006-01-02 15:04:05"), name, runtime.NumGoroutine(), len(lines))
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
