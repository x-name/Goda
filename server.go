package main

import (
	"bytes"
	"compress/zlib"
	"encoding/gob"
	"fmt"
	"github.com/valyala/fasthttp"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	//"sync"
	"time"
)

type Handler struct {
	foobar string
}

type WAL struct {
	Value     Value
	ID        uint32
	IndexName string
}

//var mutexAPI = &sync.RWMutex{}

func (h *Handler) HandleAPI(ctx *fasthttp.RequestCtx) {
	//func HandleAPI(w http.ResponseWriter, r *http.Request) {
	//ctx.SetConnectionClose()
	/*
	 */
	/*
		b, err := ioutil.ReadAll(r.Body)
		r.Body.Close()
		if err != nil {
			log.Println("Error read body:", err)
			return
		}
		reqJson := string(b)
	*/
	reqJson := string(ctx.PostBody())

	Req := strings.Split(string(ctx.Path()), "/")
	//Req := strings.Split(string(r.URL.Path), "/")
	if len(Req) < 3 {
		return
	}
	apiName := Req[2]
	/*
		if apiName != "set" {
			log.Println(string(r.URL.Path))
		}
	*/

	index := indexes[Req[1]]

	if index == nil {
		fmt.Fprintf(ctx, `{"Results":["No index."],"Status":false,"Size":0}`)
		return
	}
	if index.Loading {
		fmt.Fprintf(ctx, `{"Results":["Index loading."],"Status":false,"Size":0}`)
		return
	}

	switch string(apiName) {
	case "set":
		//mutexAPI.Lock()
		fmt.Fprintf(ctx, index.SetJson(reqJson))
		//log.Println("set")
		return
		//mutexAPI.Unlock()
	case "get":
		//mutexAPI.Lock()
		fmt.Fprintf(ctx, index.GetJson(reqJson))
		//log.Println("get")
		return
		//mutexAPI.Unlock()
	case "tags":
		//mutexAPI.Lock()\
		//x := index.TagsSortJson(reqJson)
		fmt.Fprintf(ctx, index.TagsSortJson(reqJson))
		//log.Println("tags")
		return
		//mutexAPI.Unlock()
	case "tree":
		//log.Println("tree")
		//mutexAPI.Lock()
		fmt.Fprintf(ctx, index.TreeSortJson(reqJson))
		return
		//mutexAPI.Unlock()
	case "cache":
		//log.Println("tree")
		//mutexAPI.Lock()
		if len(Req) != 4 {
			return
		}
		if Req[3] == "get" {
			fmt.Fprintf(ctx, index.CacheGetJson(reqJson))
		} else if Req[3] == "set" {
			fmt.Fprintf(ctx, index.CacheSetJson(reqJson))
		}
		return
		//mutexAPI.Unlock()
	default:

		return
	}
	return

	//ctx.SetStatusCode(fasthttp.StatusOK)
	//fmt.Fprintf(ctx, "Hello, world! Requested path is %q. Foobar is %q", ctx.Path(), h.foobar)
}

func (h *Handler) HandleStatus(ctx *fasthttp.RequestCtx) {
	//func HandleStatus(w http.ResponseWriter, r *http.Request) {
	//ctx.SetConnectionClose()
	//ctx.SetContentType("text/html")

	var Memory runtime.MemStats
	runtime.ReadMemStats(&Memory)
	fmt.Fprint(ctx, `<html><head>

	<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/materialize/0.97.8/css/materialize.min.css">
	<script src="https://code.jquery.com/jquery-3.1.1.min.js"></script>
	<script src="https://cdnjs.cloudflare.com/ajax/libs/materialize/0.97.8/js/materialize.min.js"></script>


		</head><body>`)

	requestStat := `<br>Set: ` + strconv.Itoa(StatCounter.SetCounter) + ` req/sec<br>Get: ` + strconv.Itoa(StatCounter.GetCounter) + ` req/sec`
	indexesString := `<table style="width: 300px">`
	for _, index := range indexes {
		if index.IndexLastID > 0 {
			indexesString += `<tr><td>` + index.Name + "</td><td>" + strconv.Itoa(int(index.IndexLastID)) + "</td></tr>"
		}
		if index.Loading == true && !strings.Contains(index.Name, ".") {
			requestStat = `<br>Loading...`
		}
	}
	indexesString += `</table>`

	StatCounter.SetCounter = 0
	StatCounter.SetLastFlush = time.Now()
	StatCounter.GetCounter = 0
	StatCounter.GetLastFlush = time.Now()

	fmt.Fprint(ctx, `<div class="container">
	<div class="row">
		<div id="jsonline">
			<h4>Status</h4>
			<b>Memory: `+FormatSize(uint64(Memory.Alloc))+`</b><br>
			`+requestStat+`
			`+indexesString+`
		</div>
		<a href="/.stop" class="button-ajax-send">Stop</a> |
		<a href="/.free" class="button-ajax-send">Free</a>
	</div>
	</div>
<script>
function auto_load(){
		$.ajax({
		  url: "/.status",
		  cache: false,
		  success: function(data){
			//json = JSON.parse(json);
			htmlData = $('<div/>').html(data).find("#jsonline").html();
			$("#jsonline").html(htmlData);
		  }
		});
}
$(document).ready(function(){
	auto_load();
	setInterval(auto_load,1*1000);
});
$('.button-ajax-send').click(function(e){
	e.preventDefault();
    $.ajax({ 
        url: this.href,
        success: function(result){
            ;
        }
    });
});
</script>
		</body></html>`)
}

func (h *Handler) HandleStop(ctx *fasthttp.RequestCtx) {
	ctx.SetConnectionClose()
	Stop()
}
func (h *Handler) HandleFreeMemory(ctx *fasthttp.RequestCtx) {
	ctx.SetConnectionClose()
	FreeMemory()
	log.Println("FreeMemory()")
}

func (h *Handler) HandleStreamer(ctx *fasthttp.RequestCtx) {
	ctx.SetConnectionClose()

}

var streamerBuffer map[string][]WAL

//var streamerMutex = &sync.RWMutex{}

func StreamerWriter() {
	//streamerMutex.Lock()
	for fileName := range streamerBuffer {
		Streamer(fileName, streamerBuffer[fileName])
		delete(streamerBuffer, fileName)
	}
	//streamerMutex.Unlock()
}
func StreamerAppend(fileName string, b WAL) bool {
	//streamerMutex.Lock()
	if streamerBuffer == nil {
		streamerBuffer = make(map[string][]WAL)
	}
	if streamerBuffer[fileName] == nil {
		//streamerBuffer[fileName] = make([]WAL, 1000000)
	}

	streamerBuffer[fileName] = append(streamerBuffer[fileName], b)
	//streamerMutex.Unlock()

	return true
}
func Streamer(replica string, wals []WAL) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(wals)
	if err != nil {
		log.Println(err)
	}
	d := buf.Bytes()

	var b bytes.Buffer
	w, _ := zlib.NewWriterLevel(&b, 1)
	w.Write(d)
	w.Close()
	d = b.Bytes()
	/*
		log.Println(`Sended:`, FormatSize(uint64(len(d))))

		c := &fasthttp.HostClient{
			Addr: replica,
		}

		req := &fasthttp.Request{}
		res := &fasthttp.Response{}
		//req := AcquireRequest()
		//replica = "http://" + replica + "/receiver"
		//log.Println(replica)
		//req.SetRequestURI("/receiver")
		//req.SetHost("127.0.0.1:6677")
		req.Header.SetHost(replica)
		req.Header.SetRequestURI("/receiver")
		req.Header.SetMethod("POST")
		req.SetBody(d)

		err := c.Do(req, res)
		if err != nil {
			//log.Println(req.String())
			log.Fatalf("Error when loading: %s", err)
		}
		res.Reset()
		req.Reset()
		res.ResetBody()
		req.ResetBody()
		fasthttp.ReleaseResponse(res)
		fasthttp.ReleaseRequest(req)
		FreeMemory()
		//log.Println(res.Body)
	*/

	url := "http://" + replica + "/.receiver"

	//log.Println(`Sended:`, FormatSize(uint64(len(d))))
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(d))
	req.Header.Set("Content-Type", "application/octet-stream")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	switch string(body) {
	case "SKIPPED":
		// duplicate send?
	case "SYNCED":
		// all good
	case "BAD":
		log.Println(string(body))
	default:
		log.Println("UNAVAILABLE")
		log.Println(string(body))
	}
}
func (h *Handler) HandleReceiver(ctx *fasthttp.RequestCtx) {
	d := ctx.PostBody()
	//log.Println(`Received:`, FormatSize(uint64(len(d))))

	var buf bytes.Buffer
	b := bytes.NewReader(d)
	r, err := zlib.NewReader(b)
	if err != nil {
		log.Fatal("zLib decode error:", err)
	}
	r.Close()
	io.Copy(&buf, r)
	dec := gob.NewDecoder(&buf)
	var wals []WAL
	err = dec.Decode(&wals)
	if err != nil {
		log.Fatal("Gob decode error:", err)
	}

	res := ""
	//log.Println("Sets: ", len(wals))
	for _, wal := range wals {
		//log.Println(wal)
		if indexes[wal.IndexName].IndexLastID+1 == wal.ID {
			//log.Println("Consistency OK. Adding.")
			indexes[wal.IndexName].Set(wal.Value)
			if res == `SYNCED` || res == "" {
				res = `SYNCED`
			} else {
				res = `NOT COSISTENT REQUEST`
			}
		} else if indexes[wal.IndexName].IndexLastID >= wal.ID {
			//log.Println("Duplicate request? Skip.")
			if res == `SKIPPED` || res == "" {
				res = `SKIPPED`
			} else {
				res = `NOT COSISTENT REQUEST`
			}
		} else {
			log.Println("Consistency BAD.", indexes[wal.IndexName].IndexLastID, wal.ID)
			if res == `BAD` || res == "" {
				res = `BAD`
			} else {
				res = `NOT COSISTENT REQUEST`
			}
		}
	}
	fmt.Fprintf(ctx, res)
}

func Server() {
	/*
		srv := &http.Server{
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 10 * time.Second,
			Addr:         "127.0.0.1:6677",
			Transport: &http.Transport{
				DisableKeepAlives: true,
			},
		}
	*/

	/*
		http.HandleFunc("/", HandleAPI)
		http.HandleFunc("/.status", HandleStatus)
		http.ListenAndServe(Config.Storage.Listen, nil)

	*/
	handler := &Handler{
		foobar: "foobar",
	}

	m := func(ctx *fasthttp.RequestCtx) {
		switch string(ctx.Path()) {
		case "/":
			handler.HandleAPI(ctx)
		case "/.status":
			handler.HandleStatus(ctx)
		case "/.stop":
			handler.HandleStop(ctx)
		case "/.free":
			handler.HandleFreeMemory(ctx)
		case "/.streamer":
			if Config.Replication.Master {
				handler.HandleStreamer(ctx)
			}
		case "/.receiver":
			if Config.Replication.Slave {
				handler.HandleReceiver(ctx)
			}
		default:
			handler.HandleAPI(ctx)
		}
	}
	s := &fasthttp.Server{
		Handler:            m,
		Name:               "Goda DB",
		MaxRequestBodySize: 1024 * 1024 * 1024, // 1GB
	}

	s.ListenAndServe("127.0.0.1:6677") //6960 // 6677

}
