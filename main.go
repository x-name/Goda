package main

/*
cd /home/go/src/github.com/tumashov/goda
export PATH=$PATH:/usr/local/go/bin
export GOPATH=/home/go
go build
go build && ./goda

go get github.com/BurntSushi/toml
go get github.com/RoaringBitmap/roaring
go get github.com/valyala/fasthttp
go get github.com/kjk/smaz

*/

import (
	"github.com/BurntSushi/toml"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	//"os/signal"
	"runtime"
	"runtime/debug"
	//"syscall"
	//"fmt"
	"time"

	//"flag"
	_ "net/http/pprof"
	//"runtime/pprof"
)

var Config struct {
	Storage struct {
		Directory     string
		Compress      bool
		SegmentSize   int
		EffectiveMemo bool
		Listen        string
	}
	Replication struct {
		Master  bool
		Nodes   []string
		Slave   bool
		AllowIP []string
	}
	Durability struct {
		WAL       bool
		Directory string
	}
	Performance struct {
		AppendWriterPeriod   int
		TruncateWriterPeriod int
		StreamerWriterPeriod int
		FreeMemoryOnLoading  bool
	}
	Debug struct {
		Log bool
	}
}

func Goda() int {
	configFile := "config"
	if len(os.Args) == 2 {
		configFile = os.Args[1]
	}
	//testMemory()
	//return

	b, err := ioutil.ReadFile(configFile + ".toml")
	if err != nil {
		log.Fatal(err)
	}
	if _, err := toml.Decode(string(b), &Config); err != nil {
		log.Panicln("Config error: ", err)
	}
	//log.Printf("Config:\n%v\n\n", Config)

	//runtime.GOMAXPROCS(1) NEED PERFORMANCE TEST

	//debug.SetGCPercent(100)
	//debug.SetGCPercent(2000)

	go Server()

	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	start := time.Now()
	alloc := stats

	log.Println("\n")
	go http.ListenAndServe(":6060", http.DefaultServeMux)
	go ApiStart()
	/*
		go func() {
			WriteBufferWaiting := time.Duration(3000)
			t := time.NewTicker(WriteBufferWaiting * time.Millisecond)
			for range t.C {
				log.Println("NumGoroutine: ", runtime.NumGoroutine())
			}
		}()
	*/

	if Config.Performance.FreeMemoryOnLoading {
		// GC DISABLE
		debug.SetGCPercent(-1)
	}
	//testMemory()

	//os.RemoveAll(Config.Storage.Directory + "index")
	// 20000 - 6 ms tag range // optimal for tags?
	CreateIndex("index", Config.Storage.SegmentSize)
	index := SelectIndex("index")

	if Config.Performance.FreeMemoryOnLoading {
		//FreeMemory()
		// GC ENABLE
		debug.SetGCPercent(100)
	}

	/*
		go func(index *Index) {
			WriteBufferWaiting := time.Duration(Config.Performance.TruncateWriterPeriod)
			t := time.NewTicker(WriteBufferWaiting * time.Millisecond)
			for range t.C {
				index.BitmapWriter()
			}
		}(index)
	*/
	//defer log.Println("test exit")
	go func() {
		WriteBufferWaiting := time.Duration(Config.Performance.AppendWriterPeriod)
		t := time.NewTicker(WriteBufferWaiting * time.Millisecond)
		for range t.C {
			Writer()
		}
	}()
	go func() {
		WriteBufferWaiting := time.Duration(Config.Performance.TruncateWriterPeriod)
		t := time.NewTicker(WriteBufferWaiting * time.Millisecond)
		for range t.C {
			WriterTruncate()
		}
	}()
	go func() {
		WriteBufferWaiting := time.Duration(Config.Performance.StreamerWriterPeriod)
		t := time.NewTicker(WriteBufferWaiting * time.Millisecond)
		for range t.C {
			StreamerWriter()
		}
	}()

	/*
		osSig := make(chan os.Signal, 2)
		signal.Notify(osSig, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-osSig
			log.Println("SIGTERM")
			Writer()
			WriterTruncate()
			os.Exit(1)
		}()
	*/

	//if index.SegN < 1000 {
	//go testSet(index)
	//go testGetByHash(index)
	//}
	//testCache(index)

	/*
		BitmapRead([]byte("t/Tag 11"))
		BitmapRead([]byte("t/Tag 54"))
		BitmapRead([]byte("t/Tag 125"))
		BitmapRead([]byte("t/Tag 965"))

		index.Search("1")
		index.Search("3452")
		index.Search("23456")
		index.Search("241532")
		index.Search("171476")
	*/

	wt := time.NewTicker(1000 * time.Millisecond)
	for range wt.C {
		break
	}
	//testGetIndex(index)
	//testTreeSort(index)

	/*
		testBitmap()
		testBtree()
		testRadix()
		testSearchFull()
		testGetByHash(index)
		testUpdate(index)
		testSearchFull()
		testBtree()
		testCompress()
		testDiskHash()
	*/

	//start2 := time.Now()
	//runtime.GC()
	//debug.FreeOSMemory()
	//FreeMemory()
	//log.Printf("GC: %s\n", time.Since(start2))
	runtime.ReadMemStats(&stats)
	//log.Printf("%#v", stats)
	logStat("Loaded", int(index.IndexLastID), int(index.IndexLastID), start, 1, alloc.Alloc, stats.Alloc)
	//logStat("Total", int(index.IndexLastID), int(index.IndexLastID), start, 1, alloc.TotalAlloc, stats.TotalAlloc)
	//logStat("Heap", int(index.IndexLastID), int(index.IndexLastID), start, 1, alloc.HeapAlloc, stats.HeapAlloc)
	//logStat("StackSys", int(index.IndexLastID), int(index.IndexLastID), start, 1, alloc.StackSys, stats.StackSys)
	//log.Println("%v#", stats)
	//logStat("BySize", int(index.IndexLastID), int(index.IndexLastID), start, 1, alloc.BySize.Size, stats.BySize.Size)
	//start = time.Now()
	//alloc = stats

	//index.TreeRead()
	//runtime.ReadMemStats(&stats)
	//log.Printf("%#v", stats)
	//logStat("TreeRead", len(index.IndexBitmap), len(index.IndexBitmap), start, 1, alloc.Alloc, stats.Alloc)
	//logStat("TreeRead", len(index.IndexBitmap), len(index.IndexBitmap), start, 1, alloc.TotalAlloc, stats.TotalAlloc)
	//start = time.Now()
	//alloc = stats
	/*
		t := time.NewTicker(30 * time.Second)
		for range t.C {

		}
	*/
	return 1
}
func main() {
	/*
		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc,
			os.Interrupt)
		go func() {
			//s := <-sigc
			//Stop()
			log.Println("exit now")
			//os.Exit(1)
		}()
	*/

	//os.Exit(
	Goda()
	//)
	t := time.NewTicker(30 * time.Second)
	for range t.C {

	}
}

func Stop() {
	Writer()
	WriterTruncate()
	os.Exit(1)
	//log.Println("exit now")
}
