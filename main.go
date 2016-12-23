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
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/debug"
	"time"
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

	b, err := ioutil.ReadFile(configFile + ".toml")
	if err != nil {
		log.Fatal(err)
	}
	if _, err := toml.Decode(string(b), &Config); err != nil {
		log.Panicln("Config error: ", err)
	}

	go Server()

	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	start := time.Now()
	alloc := stats

	log.Println("\n")
	go http.ListenAndServe(":6060", http.DefaultServeMux)

	if Config.Performance.FreeMemoryOnLoading {
		// GC DISABLE
		debug.SetGCPercent(-1)
	}

	//os.RemoveAll(Config.Storage.Directory + "index")
	CreateIndex("index", Config.Storage.SegmentSize)
	index := SelectIndex("index")

	if Config.Performance.FreeMemoryOnLoading {
		//FreeMemory()
		// GC ENABLE
		debug.SetGCPercent(100)
	}

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

	runtime.ReadMemStats(&stats)
	logStat("Loaded", int(index.IndexLastID), int(index.IndexLastID), start, 1, alloc.Alloc, stats.Alloc)

	return 1
}
func main() {
	Goda()

	t := time.NewTicker(30 * time.Second)
	for range t.C {

	}
}

func Stop() {
	Writer()
	WriterTruncate()
	os.Exit(1)
}
