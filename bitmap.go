package main

import (
	"bytes"
	"github.com/RoaringBitmap/roaring"
	"log"
	//"math/rand"
	"bufio"
	"encoding/gob"
	"os"
	//"sync"
	//"time"
	"encoding/binary"
	"math"
	//"runtime"
	"strconv"
	//"time"
)

//var bitmapMutex = &sync.RWMutex{}
var bitmapSaved = false

func (index *Index) BitmapWriter() {
	return
	if !bitmapSaved {
		var buf bytes.Buffer

		//bitmapMutex.Lock()

		enc := gob.NewEncoder(&buf)
		err := enc.Encode(index.IndexBitmap)
		if err != nil {
			log.Println(err)
		}

		f, err := os.OpenFile(index.File+"/.bitmaps", os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			log.Println(err)
		}
		defer f.Close()
		w := bufio.NewWriter(f)
		w.Write(buf.Bytes())
		err = w.Flush()

		bitmapSaved = true

		//bitmapMutex.Unlock()
	}
}
func (index *Index) BitmapAdd(name []byte, val uint32) {
	var key [8]byte = byteToByte8(name[:8])

	index.BitmapOpen(name)

	index.IndexBitmap[key].Add(val)

	index.BitmapIndex.SetRaw(append(uInt32Byte4(val), name...), 0)
}
func (index *Index) BitmapOpen(name []byte) {
	key := byteToByte8(name[:8])
	if index.IndexBitmap[key] == nil {
		index.IndexBitmap[key] = roaring.NewBitmap()
	}
}

func (index *Index) BitmapRead(name []byte) {
	key := byteToByte8(name[:8])

	index.BitmapOpen(name)

	log.Println("...")

	i := index.IndexBitmap[key].Iterator()
	for i.HasNext() {
		//i.Next()
		log.Println(i.Next())
	}
}

func (index *Index) LoadBitmap() bool {
	godaStat := StatStart()

	indexNmax := int(math.Ceil(float64(index.BitmapIndex.SegN) / float64(index.BitmapIndex.SegL)))
	for indexN := 1; indexN <= indexNmax; indexN++ {
		f, err := os.OpenFile(index.File+"/.bitmap/.storage/"+strconv.Itoa(indexN), os.O_RDONLY, 0600)
		if err != nil {
			log.Println(err)
			return false
		}
		stat, err := f.Stat()
		b := make([]byte, stat.Size())
		if _, err = f.Read(b); err != nil {
			log.Fatal(err)
		}
		f.Close()

		strokes := bytes.Split(b, bDelimeter)
		for _, v := range strokes {
			if len(v) <= 5 {
				continue
			}
			size := binary.LittleEndian.Uint32(v[0:4])
			if uint32(len(v[4:])) != size {
				continue
			}

			//log.Println(string(v[8:]))
			//log.Println(byte4UInt32(v[4:8]))
			val := byte4UInt32(v[4:8])
			name := []byte(string(v[8:]))
			//log.Println(name)
			var key [8]byte = byteToByte8(name[:8])

			index.BitmapOpen(name)

			//bitmapSaved = false
			index.IndexBitmap[key].Add(val)

			//index.BitmapIndex.SetRaw(append(uInt32Byte4(val), name...), 0)
		}
		//log.Panic(len(strokes))
		if Config.Performance.FreeMemoryOnLoading {
			FreeMemory()
		}
	}

	godaStat = StatEnd(godaStat, "LoadBitmap", index.BitmapIndex.SegN)

	for k := range index.IndexBitmap {
		index.IndexInverted[byteToByte8(k[:8])] = true
	}

	StatEnd(godaStat, "LoadBitmap/IndexInverted", index.BitmapIndex.SegN)

	return true
}
