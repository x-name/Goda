package main

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"log"
	"math"
	"os"
	"strconv"
)

func (index *Index) GetByHash(key []byte) *Value {
	value := new(Value)

	var keyHash []byte = make([]byte, 8)
	var keyHash4 [8]byte
	h := fnv.New64a()

	keyHash = []byte{}
	h.Write(key)
	keyHash = h.Sum(keyHash)
	copy(keyHash4[:], keyHash[:8])

	tmp := index.IndexHash[keyHash4]
	if bytes.Compare(tmp[:], b4null) == 0 {
		return nil
	} else {
		value.Data = index.GetRaw(byte4UInt32(tmp[:]))
	}

	return value
}
func (index *Index) UpdateByHash(k string, d []byte) *Value {
	value := new(Value)

	return value
}
func (index *Index) TruncateByHash(k string) *Value {
	return index.UpdateByHash(k, []byte{})
}

var hashCollisions = 0

func (index Index) HashAdd(key []byte, val []byte) bool {
	var keyHash []byte = make([]byte, 8)
	var keyHash4 [8]byte
	h := fnv.New64a()

	keyHash = []byte{}
	h.Write(key)
	keyHash = h.Sum(keyHash)
	copy(keyHash4[:], keyHash[:8])

	tmp := index.IndexHash[keyHash4]
	if bytes.Compare(tmp[:], b4null) == 0 {
		index.IndexHash[keyHash4] = byteToByte4(val)
		index.HashIndex.SetRaw(append(keyHash, val...), 0)
		return true
	} else {
		// Collision
		hashCollisions++
		//log.Println("Hash Collisions: %s", hashCollisions)
		return false
	}
}
func (index Index) HashCheck(key []byte) bool {
	var keyHash []byte = make([]byte, 8)
	var keyHash4 [8]byte
	h := fnv.New64a()

	keyHash = []byte{}
	h.Write(key)
	keyHash = h.Sum(keyHash)
	copy(keyHash4[:], keyHash[:8])

	tmp := index.IndexHash[keyHash4]
	if bytes.Compare(tmp[:], b4null) == 0 {
		return false
	} else {
		return true
	}
	return false
}

func (index *Index) LoadHash() bool {
	godaStat := StatStart()
	defer StatEnd(godaStat, "LoadHash", index.HashIndex.SegN)

	index.IndexHash = make(map[[8]byte][4]byte, index.HashIndex.SegN)

	indexNmax := int(math.Ceil(float64(index.HashIndex.SegN) / float64(index.HashIndex.SegL)))
	for indexN := 1; indexN <= indexNmax; indexN++ {
		f, err := os.OpenFile(index.File+"/.hash/.storage/"+strconv.Itoa(indexN), os.O_RDONLY, 0600)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		stat, err := f.Stat()
		b := make([]byte, stat.Size())
		if _, err = f.Read(b); err != nil {
			log.Fatal(err)
		}

		strokes := bytes.Split(b, bDelimeter)
		for _, v := range strokes {
			if len(v) <= 5 {
				continue
			}
			size := binary.LittleEndian.Uint32(v[0:4])
			if uint32(len(v[4:])) != size {
				continue
			}

			index.IndexHash[byteToByte8(v[4:12])] = byteToByte4(v[12:16])
		}

		if Config.Performance.FreeMemoryOnLoading {
			FreeMemory()
		}
	}

	return true
}
