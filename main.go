package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/geo/s2"
	"github.com/hashicorp/golang-lru/v2"
	"github.com/tidwall/gjson"
	bolt "go.etcd.io/bbolt"
)

// https://s2geometry.io/resources/s2cell_statistics.html
/*
level 	min area 	max area 	average area 	units 	  	Random cell 1 (UK) min edge length 	Random cell 1 (UK) max edge length 	  	Random cell 2 (US) min edge length 	Random cell 2 (US) max edge length 	Number of cells

17 	2970.02 	6227.43 	4948.29 	m2 	  	53 m 	74 m 	  	70 m 	77 m 	103B
18 	742.50 	1556.86 	1237.07 	m2 	  	27 m 	37 m 	  	35 m 	38 m 	412B
19 	185.63 	389.21 	309.27 	m2 	  	13 m 	19 m 	  	18 m 	19 m 	1649B
20 	46.41 	97.30 	77.32 	m2 	  	7 m 	9 m 	  	9 m 	10 m 	7T
21 	11.60 	24.33 	19.33 	m2 	  	3 m 	5 m 	  	4 m 	5 m 	26T
22 	2.90 	6.08 	4.83 	m2 	  	166 cm 	2 m 	  	2 m 	2 m 	105T
23 	0.73 	1.52 	1.21 	m2 	  	83 cm 	116 cm 	  	110 cm 	120 cm 	422T
24 	0.18 	0.38 	0.30 	m2 	  	41 cm 	58 cm 	  	55 cm 	60 cm 	1689T
25 	453.19 	950.23 	755.05 	cm2 	  	21 cm 	29 cm 	  	27 cm 	30 cm 	7e15
26 	113.30 	237.56 	188.76 	cm2 	  	10 cm 	14 cm 	  	14 cm 	15 cm 	27e15
27 	28.32 	59.39 	47.19 	cm2 	  	5 cm 	7 cm 	  	7 cm 	7 cm 	108e15
28 	7.08 	14.85 	11.80 	cm2 	  	2 cm 	4 cm 	  	3 cm 	4 cm 	432e15
29 	1.77 	3.71 	2.95 	cm2 	  	12 mm 	18 mm 	  	17 mm 	18 mm 	1729e15
30 	0.44 	0.93 	0.74 	cm2 	  	6 mm 	9 mm 	  	8 mm 	9 mm 	7e18
*/
var defaultCellLevel = 23 // =~ 1.2m^2
var defaultOutputRoot = filepath.Join(".", "output")

var flagCellLevel = flag.Int("cell-level", defaultCellLevel, fmt.Sprintf("S2 Cell Level (0-30, default=%d)", defaultCellLevel))
var flagCompressionLevel = flag.Int("compression-level", gzip.DefaultCompression, fmt.Sprintf("Compression level (Default=%d, BestSpeed=%d, BestCompression=%d)", gzip.DefaultCompression, gzip.BestSpeed, gzip.BestCompression))
var flagCacheSize = flag.Int("cache-size", 1_000_000, "Size of cache for unique cat/tracks indexed by cellID")
var flagWorkers = flag.Int("workers", 8, "Number of workers")
var flagBatchSize = flag.Int("batch-size", 1000, "Number of cat/lines per batch")
var flagOutputRootFilepath = flag.String("output", defaultOutputRoot, "Output root dir")
var flagDBRootPath = flag.String("db-root", filepath.Join(defaultOutputRoot, "dbs"), "Path to root dir for unique cat/tracks index dbs (one per cat)")
var flagDuplicateQuitLimit = flag.Int("duplicate-quit-limit", 0, "Number of sequential duplicate tracks to quit after")

var aliases = map[*regexp.Regexp]string{
	regexp.MustCompile("(?i)(Rye.*|Kitty.*|jl)"):                          "rye",
	regexp.MustCompile(`(?i)(.*Papa.*|P2|Isaac.*|.*moto.*|iha|ubp52)`):    "ia",
	regexp.MustCompile(`(?i)(Big.*Ma.*)`):                                 "jr",
	regexp.MustCompile("(?i)Kayleigh.*"):                                  "kd",
	regexp.MustCompile("(?i)(KK.*|kek)"):                                  "kk",
	regexp.MustCompile("(?i)Bob.*"):                                       "rj",
	regexp.MustCompile("(?i)(Pam.*|Rathbone.*)"):                          "pr",
	regexp.MustCompile("(?i)(Ric|.*A3_Pixel_XL.*|.*marlin-Pixel-222d.*)"): "ric",
	regexp.MustCompile("(?i)Twenty7.*"):                                   "mat",
	regexp.MustCompile("(?i)(.*Carlomag.*|JLC|jlc)"):                      "jlc",
}

func aliasOrName(name string) string {
	if name == "" {
		return "_"
	}
	for r, a := range aliases {
		if r.MatchString(name) {
			return a
		}
	}
	return name
}

func sanitizeName(name string) string {
	if name == "" {
		return "_"
	}
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, string(filepath.Separator), "_")
	return name
}

func mustGetCatName(line string) string {
	value := gjson.Get(line, "properties.Name").String()
	return sanitizeName(aliasOrName(value))
}

var dbs = make(map[string]*bolt.DB)
var cache *lru.Cache[string, bool]

func dbBucket() []byte {
	return []byte{byte(*flagCellLevel)}
}

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
}

func readLinesBatchingCats(raw io.Reader, batchSize int, workers int) (chan [][]byte, chan error, error) {
	bufferedContents := bufio.NewReaderSize(raw, 4096) // default 4096

	ch := make(chan [][]byte, workers)
	errs := make(chan error)

	go func(ch chan [][]byte, errs chan error, contents *bufio.Reader) {
		defer func(ch chan [][]byte, errs chan error) {
			// close(ch)
			close(errs)
		}(ch, errs)

		catBatches := map[string][][]byte{}
		for {
			next, err := contents.ReadBytes('\n')
			if err != nil {
				// Send remaining lines, an error expected when EOF.
				for cat, lines := range catBatches {
					if len(lines) > 0 {
						ch <- lines
						delete(catBatches, cat)
					}
				}
				errs <- err
				if err != io.EOF {
					return
				}
			} else {
				name := mustGetCatName(string(next))
				if _, ok := catBatches[name]; !ok {
					catBatches[name] = [][]byte{}
				}
				catBatches[name] = append(catBatches[name], next)
				if len(catBatches[name]) >= batchSize {
					ch <- catBatches[name]
					catBatches[name] = [][]byte{}
				}
			}
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

	gzW, _ := gzip.NewWriterLevel(fi, *flagCompressionLevel)
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

// cellIDWithLevel returns the cellID truncated to the given level.
// Levels should be [1..30].
// Level 30 gives a cell size of ~0.74cm^2.
// Level 23 gives a cell size of ~1.21m^2.
// Level 20 gives a cell size of ~77.32m^2.
// Level 17 gives a cell size of ~4948.29m^2.
// Level 14 gives a cell size of ~316.23km^2.
// Level 11 gives a cell size of ~20,199.01km^2.
// Level 8 gives a cell size of ~1,295,997.92km^2.
// Level 5 gives a cell size of ~83,138,957.98km^2.
// Level 2 gives a cell size of ~5,331,214,722.07km^2.
// Level 1 gives a cell size of ~213,248,589,882.88km^2.
// Gosh I just love the metric system.
// And AI comments.
func cellIDWithLevel(cellID s2.CellID, level int) s2.CellID {
	// https://docs.s2cell.aliddell.com/en/stable/s2_concepts.html#truncation
	var lsb uint64 = 1 << (2 * (30 - level))
	truncatedCellID := (uint64(cellID) & -lsb) | lsb
	return s2.CellID(truncatedCellID)
}

// getTrackCellID returns the cellID for the given track line, which is a raw string of a JSON-encoded geojson cat track.
// It applies the global cellLevel to the returned cellID.
func getTrackCellID(line string) s2.CellID {
	var coords []float64
	// Use GJSON to avoid slow unmarshalling of the entire line.
	gjson.Get(line, "geometry.coordinates").ForEach(func(key, value gjson.Result) bool {
		coords = append(coords, value.Float())
		return true
	})
	return cellIDWithLevel(s2.CellIDFromLatLng(s2.LatLngFromDegrees(coords[1], coords[0])), *flagCellLevel)
}

func getTrackCellIDs(trackLines [][]byte) (cellIDs []s2.CellID) {
	for _, line := range trackLines {
		cellIDs = append(cellIDs, getTrackCellID(string(line)))
	}
	return cellIDs
}

func cellIDCacheKey(cat string, cellID s2.CellID) string {
	return fmt.Sprintf("%s:%s", cat, cellID.ToToken())
}

func cellIDDBKey(cat string, cellID s2.CellID) []byte {
	return []byte(cellID.ToToken())
}

// trackInCacheByCellID returns true if the given cellID for some cat exists in the LRU cache.
// If not, it will be added to the cache.
func trackInCacheByCellID(cat string, cellID s2.CellID) (exists bool) {
	key := cellIDCacheKey(cat, cellID)
	if _, ok := cache.Get(key); ok {
		return ok
	}
	cache.Add(key, true)
	return false
}

// uniqIndexesFromCache returns the indices of the given cellIDs that are not in the LRU cache.
func uniqIndexesFromCache(cat string, cellIDs []s2.CellID) (uniqIndices []int) {
	for i, cellID := range cellIDs {
		if !trackInCacheByCellID(cat, cellID) {
			uniqIndices = append(uniqIndices, i)
		}
	}
	return uniqIndices
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
	db, err := bolt.Open(filepath.Join(*flagDBRootPath, fmt.Sprintf("%s.db", cat)), 0600, nil)
	if err != nil {
		log.Fatalln(err)
	}
	dbs[cat] = db
	return db
}

// dbWriteCellIDs writes the given cellIDs to the index db for the given cat.
// TODO, maybe do something with the value stored.
func dbWriteCellIDs(cat string, cellIDs []s2.CellID) (err error) {
	db := getOrInitCatDB(cat)
	// defer db.Close() // nope, don't close it

	bucket := dbBucket()
	return db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		for _, cellID := range cellIDs {
			err = b.Put(cellIDDBKey(cat, cellID), []byte{0x1})
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// filterUniqFromDB filters the cellIDs and tracklines that are not in the db.
func filterUniqFromDB(cat string, cellIDs []s2.CellID, trackLines [][]byte) (uniqCellIDs []s2.CellID, uniqTracklines [][]byte, err error) {
	db := getOrInitCatDB(cat)
	// defer db.Close()

	bucket := dbBucket()
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			// if no bucket has been yet created, all the cellIDs are unique
			uniqCellIDs = make([]s2.CellID, len(cellIDs))
			uniqTracklines = make([][]byte, len(cellIDs))
			copy(uniqCellIDs, cellIDs)
			copy(uniqTracklines, trackLines)
			return nil
		}
		for i, cellID := range cellIDs {
			v := b.Get(cellIDDBKey(cat, cellID))
			if v == nil {
				uniqCellIDs = append(uniqCellIDs, cellID)
				uniqTracklines = append(uniqTracklines, trackLines[i])
			}
		}
		return nil
	})
	return
}

// filterUniqCatTracks returns unique cellIDs and associated tracks for the given cat.
// It attempts first to cross reference all tracks against the cache. This step returns the indices of the tracks that are not in the cache.
// Those uncached tracks are then cross referenced against the index db.
func filterUniqCatTracks(cat string, trackLines [][]byte) (uniqCellIDs []s2.CellID, uniqTracks [][]byte) {
	if len(trackLines) == 0 {
		log.Println("WARN: no lines to dedupe")
		return uniqCellIDs, uniqTracks
	}

	// create a cellid slice analogous to trackLines
	cellIDs := getTrackCellIDs(trackLines)
	if len(cellIDs) != len(trackLines) {
		log.Fatalln("len(cellIDs) != len(trackLines)", len(cellIDs), len(trackLines))
	}

	// returns the indices of uniq tracklines (== uniq cellIDs)
	uniqCellIDTrackIndices := uniqIndexesFromCache(cat, cellIDs) // eg. 0, 23, 42, 99

	// if there are no uniq cellIDs, return early
	if len(uniqCellIDTrackIndices) == 0 {
		return
	}

	tmpUniqCellIDs := make([]s2.CellID, len(uniqCellIDTrackIndices))
	tmpUniqTracks := make([][]byte, len(uniqCellIDTrackIndices))
	for ii, idx := range uniqCellIDTrackIndices {
		tmpUniqCellIDs[ii] = cellIDs[idx]
		tmpUniqTracks[ii] = trackLines[idx]
	}

	// so now we've whittled the tracks to only those not in the cache
	// we need to check the db for those that did not have cache hits

	// further whittle the uniqs based on db hits/misses
	_uniqCellIDs, _uniqTracks, err := filterUniqFromDB(cat, tmpUniqCellIDs, tmpUniqTracks)
	if err != nil {
		log.Fatalln(err)
	}
	return _uniqCellIDs, _uniqTracks
}

// getTrackTime returns the Time value from the geojson track.
// It's a helper function for pretty logging.
func getTrackTime(line []byte) time.Time {
	return time.Time(gjson.GetBytes(line, "properties.Time").Time())
}

var globalContinuousDuplicatesCounter = uint32(0)

func main() {
	flag.Parse()

	cache, _ = lru.New[string, bool](*flagCacheSize)
	defer func() {
		for cat, db := range dbs {
			log.Printf("Closing DB %s\n", cat)
			db.Close()
		}
	}()

	// ensure output dirs exists
	_ = os.MkdirAll(*flagOutputRootFilepath, 0755)
	_ = os.MkdirAll(*flagDBRootPath, 0755)

	// workersWG is used for clean up processing after the reader has finished.
	workersWG := new(sync.WaitGroup)
	type work struct {
		name  string
		lines [][]byte
	}
	workCh := make(chan work, *flagWorkers)
	workerFn := func(workerI int, w work) {
		defer workersWG.Done()
		start := time.Now()

		// fname is the name where unique tracks will be appended
		fname := filepath.Join(*flagOutputRootFilepath, fmt.Sprintf("%s.level-%d.json.gz", w.name, *flagCellLevel))

		workLogger := log.New(os.Stderr, fmt.Sprintf("[worker %02d] name=%s file=%s lines=%d ", workerI, w.name, fname, len(w.lines)), log.Lshortfile|log.LstdFlags)

		// dedupe: x-check the cache and db for existing cells for these tracks.
		// only unique tracks are returned.
		uCellIDs, uTracks := filterUniqCatTracks(w.name, w.lines)
		if len(uCellIDs) != len(uTracks) {
			log.Fatalln("len(uCellIDs) != len(uTracks)", len(uCellIDs), len(uTracks))
		}
		workLogger.Printf("uniq=%d took=%v\n", len(uCellIDs), time.Since(start))

		// noop if 0 unique lines
		if len(uCellIDs) == 0 {
			atomic.AddUint32(&globalContinuousDuplicatesCounter, uint32(len(w.lines)))
			if *flagDuplicateQuitLimit > 0 && atomic.LoadUint32(&globalContinuousDuplicatesCounter) > uint32(*flagDuplicateQuitLimit) {
				log.Printf("Found %d consecutive duplicates, quitting...\n", globalContinuousDuplicatesCounter)
				os.Exit(0)
			}
			return
		}
		atomic.StoreUint32(&globalContinuousDuplicatesCounter, 0)

		start = time.Now()
		// write the unique tracks to the index db
		// the cache-check function will have already stored the unique cellIDs in the LRU cache
		if err := dbWriteCellIDs(w.name, uCellIDs); err != nil {
			log.Fatalln(err)
		}

		// finally, append the unique tracks to the per-cat unique tracks file
		if err := appendGZipLines(uTracks, fname); err != nil {
			log.Fatalln(err)
		}
		workLogger.Printf("indexed and appended, took=%s\n", time.Since(start))
	}

	for i := 0; i < *flagWorkers; i++ {
		workerI := i + 1
		go func() {
			workerI := workerI
			for w := range workCh {
				workerFn(workerI, w)
			}
		}()
	}

	// these only used for pretty logging, to see track time progress
	latestTrackTimePrefix := time.Time{}
	lastTimeLogPrefixUpdate := time.Now()

	prettyLogging := func(lines [][]byte) {
		// update latest track time progress if stale
		// don't do for every because it's unnecessarily expensive
		if latestTrackTimePrefix.IsZero() || time.Since(lastTimeLogPrefixUpdate) > time.Second*5 {
			latestTrackTimePrefix = getTrackTime(lines[0])
			log.SetPrefix(fmt.Sprintf("🐈 %s | ", latestTrackTimePrefix.Format("2006-01-02")))
			lastTimeLogPrefixUpdate = time.Now()
		}
	}

	handleLinesBatch := func(lines [][]byte) {
		prettyLogging(lines)
		cat := mustGetCatName(string(lines[0]))
		log.Printf("BATCH %s GOROUTINES %d LINES %d", cat, runtime.NumGoroutine(), len(lines))
		workersWG.Add(1)
		workCh <- work{name: cat, lines: lines} //
	}

	linesCh, errCh, err := readLinesBatchingCats(os.Stdin, *flagBatchSize, *flagWorkers)
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
	for linesCh != nil && len(linesCh) > 0 {
		handleLinesBatch(<-linesCh)
	}
	close(linesCh)
	log.Println("Waiting for workers to finish...")
	workersWG.Wait()
	close(workCh)
}
