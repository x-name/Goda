package main

import (
	//"bytes"
	"log"
	"os"
	"strconv"
	//"sync"
	//"reflect"
	//"github.com/emirpasic/gods/maps/treemap"
	"bytes"
	"encoding/gob"
	"errors"
	"math"
	//"sync"
	"time"
)

var indexLen uint32
var indexMemoLen uint32

var ReplicationConcurrency int = 10
var ReplicationSem chan bool = make(chan bool, ReplicationConcurrency)

type statCounter struct {
	SetCounter   int
	SetLastFlush time.Time
	GetCounter   int
	GetLastFlush time.Time
}

var StatCounter statCounter

func (index *Index) Set(value Value) ([]byte, error) {
	StatCounter.SetCounter++

	if value.Options.HashDuplicate != 2 {
		if len(value.Hash) > 0 {
			var kString string
			if value.Options.HashDuplicate == 0 {
				for _, k := range value.Hash {
					if index.HashCheck([]byte(k)) {
						return []byte{}, errors.New("Hash duplicate.")
					}
				}
			} else if value.Options.HashDuplicate == 1 {
				for _, k := range value.Hash {
					kString = k
					break
				}
				if index.HashCheck([]byte(kString)) {
					return []byte{}, errors.New("Hash duplicate.")
				}
			} else {
				return []byte{}, errors.New("Wrong value HashDuplicate.")
			}
		}
	}

	var key []byte

	if len(value.Data) > 0 {
		index.IndexLastID++
		key = uInt32Byte4(index.IndexLastID)
		if Config.Durability.WAL {
			go func() {
				wal := value

				var buf bytes.Buffer
				enc := gob.NewEncoder(&buf)
				err := enc.Encode(wal)
				if err != nil {
					log.Println(err)
				}

				WriteAppend(Config.Durability.Directory+"index/data.wal", append(bDelimeter, append(key, buf.Bytes()...)...))
			}()
		}

		if index.IndexLastID >= indexLen {
			index.Index = append(index.Index, make([][8]byte, 100000)...)
			indexLen += 100000
		}

		index.Index[index.IndexLastID] = index.SetRaw(value.Data, value.Options.Reserve)

		index.IndexIndex.SetRaw(index.Index[index.IndexLastID][:], 0)
		index.BitmapAdd([]byte("t/.index"), index.IndexLastID)
	} else {
		return []byte{}, errors.New("No data.")
	}

	if len(value.Memo) > 0 {
		index.MemoIndex.SetRaw(append(key, value.Memo...), 0)

		if Config.Storage.EffectiveMemo {
			if index.IndexLastID >= indexMemoLen {
				index.IndexEffectiveMemo = append(index.IndexEffectiveMemo, make([]string, 100000)...)
				indexMemoLen += 100000
			}
			index.IndexEffectiveMemo[index.IndexLastID] = string(index.CompressMemo(value.Memo))
		} else {
			index.IndexMemo[byteToByte4(key)] = string(index.CompressMemo(value.Memo))
		}
	}

	//return false

	for _, k := range value.Hash {
		if !index.HashAdd([]byte(k), key) {
			// Collision
		}
	}

	for k, v := range value.Tree {
		kByte := []byte(k)[:8]
		indexTreeMapName := byteToByte8(kByte)
		//kString := string(kByte)
		//if index.Tree[kString] == nil {
		//	index.Tree[k] = treemap.NewWithIntComparator()
		//}
		var bHash []byte = append(kByte, bDelimeter2...)
		bHash = append(bHash, uInt64Byte8(uint64(v))...)
		bHash = append(bHash, bDelimeter2...)
		bHash = append(bHash, key...)
		index.TreeIndex.SetRaw(bHash, 0)
		// overflow
		/*
			var bHash []byte = append(kByte, bDelimeter2...)
			bHash = append(bHash, uInt64Byte8(uint64(v))...)
			bHash = append(bHash, bDelimeter2...)
			bHash = append(bHash, key...)
			index.TreeIndex.SetRaw(bHash, 0)
		*/
		//index.Tree[k].Put(v, Escape(key))

		index.BitmapAdd([]byte("o/"+k), uint32(v))

		indexTreeMapKey := byteToByte4(uInt32Byte4(uint32(v)))
		if index.IndexTree[indexTreeMapName] == nil {
			index.IndexTree[indexTreeMapName] = make(map[[4]byte][][4]byte, 1)
		}
		if index.IndexTree[indexTreeMapName][indexTreeMapKey] == nil {
			index.IndexTree[indexTreeMapName][indexTreeMapKey] = make([][4]byte, 1)
			index.IndexTree[indexTreeMapName][indexTreeMapKey][0] = byteToByte4(key)
		} else {
			indexTreeMapKeyN := len(index.IndexTree[indexTreeMapName][indexTreeMapKey])
			index.IndexTree[indexTreeMapName][indexTreeMapKey] = append(index.IndexTree[indexTreeMapName][indexTreeMapKey], make([][4]byte, 1)...)
			index.IndexTree[indexTreeMapName][indexTreeMapKey][indexTreeMapKeyN] = byteToByte4(key)
		}
	}

	for _, t := range value.Tags {
		index.BitmapAdd([]byte("t/"+t), index.IndexLastID)
	}

	for _, s := range value.Full {
		index.InvertedIndex(key, s)
	}

	// Data (1KB) + Index + Hash + Tree + Inverted + Bitmaps
	// Write 55,000 r/s
	///^ 1,000,000; Memory 50MB(heap?) (after GC 0 heap?) 130-220MB(Alloc?) (after GC 44MB Alloc?) 190-210MB(TotalAlloc?) (after GC RANDOM, 0 TotalAlloc?); Storage 1GB
	// ^ 10,000,000; Memory 800MB-1.5GB; Storage 11GB; Adding time 4 min
	// ^ 30,000,000; Memory 2.2GB; Storage 34GB; Load time 1m51s

	if Config.Durability.WAL {
		WriteTruncate(index.File+"/checkpoint.wal", key)
	}

	if Config.Replication.Master {
		go func() {
			//ReplicationSem <- true
			//go func() {
			//defer func() { <-ReplicationSem }()
			wal := WAL{
				Value:     value,
				ID:        index.IndexLastID,
				IndexName: index.Name,
			}
			/*
				var buf bytes.Buffer
				enc := gob.NewEncoder(&buf)
				err := enc.Encode(wal)
				if err != nil {
					log.Println(err)
				}
			*/

			for _, Node := range Config.Replication.Nodes {
				//Streamer(Node, append(bDelimeter, append(key, buf.Bytes()...)...))
				StreamerAppend(Node, wal)
			}
			//}()
		}()
	}

	return key, nil
}

//var concurrency int = 1
//var sem chan bool = make(chan bool, concurrency)

func (index *Index) SetRaw(d []byte, reserve int) [8]byte {
	indexN := index.N
	//index.IndexIndex.SegN++
	index.SegN++
	for index.SegN-(indexN*index.SegL-index.SegL) > index.SegL {
		indexN++
	}

	if index.N != indexN {
		var err error

		index.FileOpen.Close()

		index.N = indexN
		index.FileOpen, err = os.OpenFile(index.File+"/.storage/"+strconv.Itoa(indexN), os.O_APPEND|os.O_CREATE, 0600)
		if err != nil {
			log.Fatal(err)
		}
		stat, err := index.FileOpen.Stat()
		index.FileOffset = uint32(stat.Size())
	}

	if Config.Storage.Compress {
		d = Encode(d)
	}

	sizeInt := len(d)
	if reserve > 0 {
		sizeInt += reserve
		d = append(d, make([]byte, reserve)...)
	}
	size := uInt32Byte4(uint32(sizeInt))

	data := append(bDelimeter, append(size, d...)...)

	WriteAppend(index.File+"/.storage/"+strconv.Itoa(indexN), data)
	/*
		sem <- true
		go func(index *Index, indexN int, data []byte) {
			defer func() { <-sem }()
			WriteAppend(index.File+"/.storage/"+strconv.Itoa(indexN), data)
		}(index, indexN, data)
	*/

	var bKey [8]byte
	//bKey = append(bKey, uInt32Byte4(uint32(indexN))...)
	//bKey = append(bKey, ...)
	bKeyA := append(uInt32Byte4(uint32(index.FileOffset+uint32(len(bDelimeter)))), uInt32Byte4(uint32(len(data)-len(bDelimeter)))...)
	copy(bKey[:], bKeyA[:8])

	index.FileOffset += uint32(len(data))

	//bKey = append(bKey, []byte(index.Name)...)
	/*
		bKey = append(bKey, uInt32Byte4(uint32(indexN))...)
		bKey = append(bKey, uInt32Byte4(uint32(stat.Size()+int64(len(bDelimeter))))...)
		bKey = append(bKey, uInt32Byte4(uint32(len(data)-len(bDelimeter)))...)
		bKey = append(bKey, []byte(index.Name)...)
	*/

	/*
			if _, err = index.FileOpen.Write(data); err != nil {
				log.Fatal(err)
			}
		var configUpdate []byte =
		if _, err = index.Config.WriteAt(configUpdate, 0); err != nil {
			log.Fatal(err)
		}
	*/

	WriteTruncate(index.File+"/.config", []byte(strconv.Itoa(index.SegN)+"\n"+strconv.Itoa(index.SegL)))

	//log.Println(bKey)

	return bKey
}

var lastReadFile *os.File
var lastReadFileN uint64

func (index *Index) GetRaw(id uint32) []byte {
	StatCounter.GetCounter++

	var rBytes []byte
	var err error

	if id >= uint32(len(index.Index)) {
		log.Println("GetRaw: Index out of range")
		return rBytes
	}
	k := index.Index[id]
	indexNuint64 := uint64(math.Floor(float64(id)/float64(index.SegL))) + 1
	indexN := strconv.FormatUint(indexNuint64, 10)
	if lastReadFileN != indexNuint64 || lastReadFile == nil {
		lastReadFileN = indexNuint64
		lastReadFile, err = os.OpenFile(index.File+"/.storage/"+indexN, os.O_RDONLY, 0600)
		if err != nil {
			log.Println(err)
		}
	}
	//log.Panic(index.Index[:100])
	b := make([]byte, byte4UInt32(k[4:8]))
	if _, err = lastReadFile.ReadAt(b, int64(byte4UInt32(k[0:4]))); err != nil {
		//log.Fatal(err)
	}
	/*
		bSplit := bytes.Split(b, bDelimeter)
		if len(bSplit) == 2 {
			b = bSplit[1]
		} else {
			log.Println("Bad Storage-file split")
			//log.Println(b)
			//log.Println(bSplit)
			//rBytes = Decode(b[4:])
			return rBytes
		}
	*/
	//log.Println(byte4UInt32(b[0:4])) // Size
	if len(b) > 4 {
		if byte4UInt32(b[0:4]) == uint32(len(b[4:])) {
			if Config.Storage.Compress {
				rBytes = Decode(b[4:])
			} else {
				rBytes = b[4:]
			}
			//log.Println(string(rBytes))
		} else {
			//log.Println("Bad Storage-file segment")
			return rBytes
		}
	} else {
		//log.Println("Bad OMG Storage-file segment", b, k, index.Index[id])
		return rBytes
	}

	return rBytes
}

var lastUpdateFile *os.File
var lastUpdateFileN uint64

func (index *Index) UpdateRaw(k []byte, d []byte) []byte {
	var rBytes []byte
	var err error

	indexNuint64 := uint64(byte4UInt32(k[0:4]))
	indexN := strconv.FormatUint(indexNuint64, 10)
	if lastReadFileN != indexNuint64 {
		lastReadFileN = indexNuint64
		lastReadFile, err = os.OpenFile(index.File+"/.storage/"+indexN, os.O_RDONLY, 0600)
		if err != nil {
			log.Println(err)
		}
	}
	sizeByte := k[8:12]
	size := byte4UInt32(sizeByte)
	off := byte4UInt32(k[4:8])
	b := make([]byte, size)
	if _, err = lastReadFile.ReadAt(b, int64(off)); err != nil {
		log.Fatal(err)
	}

	if len(d) > 0 {
		d = Encode(d)
	}
	if size >= uint32(len(d)) {
		//log.Println("Add new version (Encoded): " + strconv.Itoa(int(len(d))) + " bytes; Available size: " + strconv.Itoa(int(size)) + " bytes")

		if lastUpdateFileN != indexNuint64 {
			lastUpdateFileN = indexNuint64
			lastUpdateFile, err = os.OpenFile(index.File+"/.storage/"+indexN, os.O_WRONLY, 0600)
			if err != nil {
				log.Println(err)
				return rBytes
			}
		}

		data := append(uInt32Byte4(size-4), d...)
		reserve := int(size) - len(data)
		if reserve > 0 {
			data = append(data, make([]byte, reserve)...)
		}

		if _, err = lastUpdateFile.WriteAt(data, int64(off)); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Println("Not enought empty space on this chunk, you can use only " + strconv.Itoa(int(size)) + " bytes; Sended: " + strconv.Itoa(int(len(d))) + " bytes")
	}

	if byte4UInt32(b[0:4]) == uint32(len(b[4:])) {
		//log.Println(b)
		//rBytes = Decode(b[4:])
	} else {
		log.Println("Bad Storage-file segment")
		return rBytes
	}
	//log.Println(string(rBytes))

	return rBytes
}
