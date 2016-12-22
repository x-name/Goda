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
	//old := s
	for _, v := range index.DictionaryMemo {
		if bytes.Contains(s, []byte(v.Key)) {
			s = bytes.Replace(s, []byte(v.Key), []byte{byte(0xFF), byte(v.Value)}, -1)
		}
	}
	//log.Println(len(old), "->", len(s), "\t Compression %", 100-float32(len(s))/float32(len(old))*100, string(old))
	//log.Println(index.DictionaryMemo)

	return s
}
func (index Index) DecompressMemo(s []byte) []byte {
	//old := s
	for _, v := range index.DictionaryMemo {
		if bytes.Contains(s, []byte{byte(0xFF), byte(v.Value)}) {
			s = bytes.Replace(s, []byte{byte(0xFF), byte(v.Value)}, []byte(v.Key), -1)
		}
	}
	//log.Println(len(old), "->", len(s), "\t Decompression %", 100-float32(len(s))/float32(len(old))*100, string(s))

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
	//log.Println(index.DictionaryMemo)

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
	//log.Println(index.DictionaryMemo)

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

/*
	//log.Println(DictionaryStruct)

	godaStat := StatStart()
	var example string
	var c []byte
	count := 1 //80000
	// Compress+Decompress / 110,000 r/s
	// Compress / 180,000 r/s
	// Decompress / 280,000 r/s
	for i := 0; i < count; i++ {
		c = Compress([]byte(example), index.DictionaryMemo)
		Decompress(c, index.DictionaryMemo)
	}
	StatEnd(godaStat, "Compress/Decompress", count)
*/
/*
func reverseMap(m map[string]string) map[string]string {
	n := make(map[string]string)
	for k, v := range m {
		n[v] = k
	}
	return n
}
func CreateDictionary(data []string) {
	//Dictionary := make(map[string]byte, 256)
	DictionaryInt := make(map[string]int, 256)
	//DictionaryInverted := make(map[byte]string, 256)

	min := 0

	dataCounting := make(map[string]int)
	for _, d := range data {
		data2 := strings.Split(d, ";")
		for _, d2 := range data2 {
			buffs := make([]string, 1)
			bLast := ""
			bLast = ""
			dataSplittedString := strings.Split(d2, ".")
			for i := 0; i < len(dataSplittedString); i++ {
				if i == 0 {
					bLast += string(dataSplittedString[i])
				} else {
					bLast += "." + string(dataSplittedString[i])
				}
				if len(bLast) < min {
					continue
				}
				buffs = append(buffs, bLast)
			}
			bLast = ""
			dataSplittedString = strings.Split(d2, "/")
			for i := 0; i < len(dataSplittedString); i++ {
				if i == 0 {
					bLast += string(dataSplittedString[i])
				} else {
					bLast += "/" + string(dataSplittedString[i])
				}
				if len(bLast) < min {
					continue
				}
				buffs = append(buffs, bLast)
			}
			bLast = ""
			for i := len(dataSplittedString) - 1; i >= 0; i-- {
				if i == len(dataSplittedString)-1 {
					bLast = string(dataSplittedString[i])
				} else {
					bLast = string(dataSplittedString[i]) + "/" + bLast
				}
				if len(bLast) < min {
					continue
				}
				buffs = append(buffs, bLast)
			}
			bLast = ""
			dataSplittedString = strings.Split(d2, "-")
			for i := 0; i < len(dataSplittedString); i++ {
				if i == 0 {
					bLast += string(dataSplittedString[i])
				} else {
					bLast += "-" + string(dataSplittedString[i])
				}
				if len(bLast) < min {
					continue
				}
				buffs = append(buffs, bLast)
			}
			bLast = ""
			dataSplittedString = strings.Split(d2, " ")
			for i := 0; i < len(dataSplittedString); i++ {
				if i == 0 {
					bLast += string(dataSplittedString[i])
				} else {
					bLast += " " + string(dataSplittedString[i])
				}
				if len(bLast) < min {
					continue
				}
				buffs = append(buffs, bLast)
			}
			//log.Println(buffs)
			for _, buff := range buffs {
				dataCounting[buff]++
			}
		}
	}

	for key, count := range dataCounting {
		if count < 3 || len(key) < 5 {
			delete(dataCounting, key)
			//dataCounting[key] = 0
			continue
		}
		dataCounting[key] = count + len(key)
	}

	//min = len(data) / 10
	prevKey := ""

	i := 0
	m := dataCounting
	n := map[int][]string{}
	var a []int
	for k, v := range m {
		if v < min {
			continue
		}
		n[v] = append(n[v], k)
	}
	for k := range n {
		a = append(a, k)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(a)))
	for _, k := range a {
		for _, s := range n[k] {
			if strings.Contains(prevKey, s) {
				//dataCounting[k] = 0
				delete(dataCounting, s)
				continue
			}
			if i > 255 || dataCounting[s] < 10 {
				delete(dataCounting, s)
				//break
				continue
			}
			//fmt.Printf("%s, %d\n", s, k)

			DictionaryInt[s] = i
			prevKey = s
			i++
		}
	}

	var Dictionary Dictionary
	Dictionary = getDictionaryFromMap(DictionaryInt)
	for _, v := range Dictionary {
		fmt.Printf("%s\n", v.Key)

	}

}
*/
