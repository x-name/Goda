package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

func (index *Index) GetMemo(id uint32) []byte {
	StatCounter.GetCounter++
	var r []byte
	if Config.Storage.EffectiveMemo {
		r = []byte(index.IndexEffectiveMemo[id])
	} else {
		r = []byte(index.IndexMemo[byteToByte4(uInt32Byte4(id))])
	}
	r = index.DecompressMemo(r)
	return r
}

type Dictionary []DictionaryKV
type DictionaryKV struct {
	Key   string
	Value int
}

func (index Index) CompressMemo(s []byte) []byte {
	for _, v := range index.DictionaryMemo {
		if bytes.Contains(s, []byte(v.Key)) {
			s = bytes.Replace(s, []byte(v.Key), []byte{byte(0xFF), byte(v.Value)}, -1)
		}
	}

	return s
}
func (index Index) DecompressMemo(s []byte) []byte {
	for _, v := range index.DictionaryMemo {
		if bytes.Contains(s, []byte{byte(0xFF), byte(v.Value)}) {
			s = bytes.Replace(s, []byte{byte(0xFF), byte(v.Value)}, []byte(v.Key), -1)
		}
	}

	return s
}

func (index Index) LoadDictionaryMemo() Dictionary {
	f, err := os.OpenFile(index.File+"/.dictionary", os.O_RDONLY, 0600)
	if err != nil {
		log.Println(err)
	}
	stat, err := f.Stat()
	b := make([]byte, stat.Size())
	if _, err = f.Read(b); err != nil {
		log.Println(err)
	}
	f.Close()

	strokes := strings.Split(string(b), "\n")
	dictionary := make(map[string]int, len(strokes))
	i := 0
	for _, v := range strokes {
		dictionary[v] = i
		i++
	}

	index.DictionaryMemo = getDictionaryFromMap(dictionary)

	return index.DictionaryMemo
}

func getDictionaryFromMap(mapStringInt map[string]int) Dictionary {
	pl := make(Dictionary, len(mapStringInt))
	i := 0
	for k, v := range mapStringInt {
		pl[i] = DictionaryKV{k, v}
		i++
	}
	sort.Sort(sort.Reverse(pl))
	return pl
}
func (p Dictionary) Len() int           { return len(p) }
func (p Dictionary) Less(i, j int) bool { return len(p[i].Key) < len(p[j].Key) }
func (p Dictionary) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (index *Index) LoadMemo() bool {
	godaStat := StatStart()
	defer StatEnd(godaStat, "LoadMemo", index.MemoIndex.SegN)

	if !Config.Storage.EffectiveMemo {
		index.IndexMemo = make(map[[4]byte]string)
	}

	indexNmax := int(math.Ceil(float64(index.MemoIndex.SegN) / float64(index.MemoIndex.SegL)))
	var indexLastID uint32 = 0
	for indexN := 1; indexN <= indexNmax; indexN++ {
		f, err := os.OpenFile(index.File+"/.memo/.storage/"+strconv.Itoa(indexN), os.O_RDONLY, 0600)
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

			if Config.Storage.EffectiveMemo {
				indexLastID = byte4UInt32(v[4:8])
				if indexLastID >= indexMemoLen {
					index.IndexEffectiveMemo = append(index.IndexEffectiveMemo, make([]string, 100000)...)
					indexMemoLen += 100000
				}
				index.IndexEffectiveMemo[indexLastID] = string(index.CompressMemo(v[8:]))
			} else {
				index.IndexMemo[byteToByte4(v[4:8])] = string(index.CompressMemo(v[8:]))
			}
		}
		if Config.Performance.FreeMemoryOnLoading {
			FreeMemory()
		}
	}

	return true
}
