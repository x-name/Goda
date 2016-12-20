package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"math"
	"os"
	//"runtime"
	"hash/fnv"
	"strconv"
	//"time"
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

	//value.Hash = []string{}
	//kByte4 := index.IndexHash[byteToByte4([]byte(k))]
	/*
		kByte4 := index.IndexHash[k]
		if bytes.Compare(kByte4[:], b4null) == 0 {
			return nil
		} else {
			//value.Data = index.GetRaw(kByte4[:])
			//log.Println(string(value.Data))
		}
	*/

	return value
}
func (index *Index) UpdateByHash(k string, d []byte) *Value {
	value := new(Value)

	/*
		//kByte4 := index.IndexHash[byteToByte4([]byte(k))]
		kByte4 := index.IndexHash[k]
		if bytes.Compare(kByte4[:], b4null) == 0 {
			return nil
		} else {
			value.Data = index.UpdateRaw(kByte4[:], d)
		}
	*/
	return value
}
func (index *Index) TruncateByHash(k string) *Value {
	return index.UpdateByHash(k, []byte{})
}

var hashCollisions = 0

func (index Index) HashAdd(key []byte, val []byte) bool {
	/*
		var keyHash []byte = make([]byte, 4)
		var keyHash4 [4]byte
		h := fnv.New32a()

		keyHash = []byte{}
		h.Write(key)
		keyHash = h.Sum(keyHash)
		copy(keyHash4[:], keyHash[:4])

		tmp := index.IndexHash[keyHash4]
		if bytes.Compare(tmp[:], b4null) == 0 {
			index.IndexHash[keyHash4] = byteToByte4(val)
			index.HashIndex.SetRaw(append(keyHash, val...), 0)
			return true
		} else {
			// Collision
			// HashIndex[keyHash4] = val
			return false
		}
	*/
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
		// HashIndex[keyHash4] = val
		hashCollisions++
		//log.Println("Hash Collisions: %s", hashCollisions)
		return false
	}
	/*
		keyHash4 := string(key)

		tmp := index.IndexHash[keyHash4]
		if bytes.Compare(tmp[:], b4null) == 0 {
			index.IndexHash[keyHash4] = byteToByte4(val)
			index.HashIndex.SetRaw(append(val, key...), 0)
			return true
		} else {
			// Collision
			// HashIndex[keyHash4] = val
			return false
		}
	*/
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
	/*
		var keyHash []byte = make([]byte, 4)
		var keyHash4 [4]byte
		h := fnv.New32a()

		keyHash = []byte{}
		h.Write(key)
		keyHash = h.Sum(keyHash)
		copy(keyHash4[:], keyHash[:4])
	*/
	/*
		keyHash4 := string(key)

		tmp := index.IndexHash[keyHash4]
		if bytes.Compare(tmp[:], b4null) != 0 {
			return true
		}
	*/
	return false
}

func (index *Index) LoadHash() bool {
	godaStat := StatStart()
	defer StatEnd(godaStat, "LoadHash", index.HashIndex.SegN)
	//defer getStat("LoadHash / 32bit", index.HashIndex.SegN, time.Now(), readMemStats())

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
			//log.Println(strokes)
			if len(v) <= 5 {
				continue
			}
			//buff := bytes.NewBuffer(v[0:4])
			size := binary.LittleEndian.Uint32(v[0:4])
			//log.Println(size)
			if uint32(len(v[4:])) != size {
				//log.Println(size)
				//log.Println(len(v[4:]))
				//log.Panic("...")
				continue
			}
			//stroke := bytes.Split(Decode(v[4:]), bDelimeter2)
			//key := string(stroke[0])
			//index.Hash[key] = make([]byte, len(stroke[1]))
			//copy(index.Hash[key], stroke[1])

			//log.Println(size)
			//log.Println(byteToByte8(v[4:12]))
			//index.IndexHash[string(v[8:])] = byteToByte4(v[4:8])
			index.IndexHash[byteToByte8(v[4:12])] = byteToByte4(v[12:16])
			//log.Println(v)
			//log.Println(byteToByte4(v[7:11]))
			//log.Println(byteToByte4(v[11:15]))
			//log.Panic(v)

			//index.Hash[string(stroke[0])] = value
			//log.Println(len(stroke[1]))
			//log.Println(len(index.Hash[string(stroke[0])]))
			//log.Println(index.Hash)
			//log.Println(string(stroke[0]))
			//log.Println(string(stroke[1]))
			//log.Panic("...")
		}
		//log.Println(index.IndexHash)
		//log.Println(len(strokes))
		//log.Println(len(index.IndexHash))
		if Config.Performance.FreeMemoryOnLoading {
			FreeMemory()
		}
	}

	return true
}
