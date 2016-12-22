package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"
	"math"
	"os"
	"strconv"

	"github.com/RoaringBitmap/roaring"
)

type Index struct {
	Name      string
	File      string
	Config    *os.File
	SegN      int
	SegL      int
	TagsIndex map[string]*Index
	HashIndex *Index
	TreeIndex *Index

	Index       [][8]byte
	IndexIndex  *Index
	Loading     bool
	IndexLastID uint32
	N           int
	FileOpen    *os.File
	FileOffset  uint32

	IndexHash     map[[8]byte][4]byte
	IndexInverted map[[8]byte]bool
	IndexTree     map[[8]byte]map[[4]byte][][4]byte

	MemoIndex          *Index
	DictionaryMemo     Dictionary
	IndexEffectiveMemo []string           // 35 bytes/entry overhead
	IndexMemo          map[[4]byte]string // 60 bytes/entry overhead

	BitmapIndex *Index
	IndexBitmap map[[8]byte]*roaring.Bitmap

	Cache map[[8]byte]struct {
		Val    string
		Expire int
	}
}

var indexes map[string]*Index = make(map[string]*Index)

func CreateIndex(k string, seg int) *Index {
	index, err := createIndex(k, seg)

	if index != nil && err == nil {
		_, _ = createIndex(index.Name+"/.hash", seg)
		_, _ = createIndex(index.Name+"/.tree", seg)
		_, _ = createIndex(index.Name+"/.storage", seg)
		_, _ = createIndex(index.Name+"/.bitmap", seg)

		_, _ = createIndex(index.Name+"/.memo", seg)
		f, err := os.OpenFile(index.File+"/.dictionary", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			log.Println(err)
		}
		_, err = f.WriteString(
			"2020-\n2019-\n2018-\n2017-\n2016-\n2015-\n2014-\n2013-\n2012-\n2011-\n2010-\n;http://\n;https://\nwww.\n.jpg\n&amp;")
		if err != nil {
			log.Fatal(err)
		}
		f.Close()
	}

	return index
}
func createIndex(k string, seg int) (*Index, error) {
	index := new(Index)
	index.Loading = true
	index.Name = k
	index.File = Config.Storage.Directory + k
	index.SegL = seg
	index.SegN = 0

	if _, err := os.Stat(index.File + "/.config"); err == nil {
		return selectIndex(k), errors.New("file exist, return selected index")
	}

	_ = os.Mkdir(Config.Storage.Directory, 0600)
	_ = os.Mkdir(index.File, 0600)
	_ = os.Mkdir(index.File+"/.storage/", 0600)

	var err error
	index.Config, err = os.OpenFile(index.File+"/.config", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		log.Println(err)
		log.Fatal("Cannot create DB: directory not empty " + index.File)
		return nil, nil
	}
	if _, err = index.Config.WriteString(strconv.Itoa(index.SegN) + "\n" + strconv.Itoa(index.SegL)); err != nil {
		log.Fatal(err)
	}
	index.Config.Close()

	f, err := os.OpenFile(index.File+"/.storage/1", os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		log.Fatal(err)
	}
	f.Close()

	return index, nil
}

func SelectIndex(k string) *Index {
	index := selectIndex(k)

	index.IndexLastID = 0

	index.IndexIndex = selectIndex(index.Name + "/.storage")
	index.LoadIndex()

	index.BitmapIndex = selectIndex(index.Name + "/.bitmap")
	index.IndexBitmap = make(map[[8]byte]*roaring.Bitmap)

	index.HashIndex = selectIndex(index.Name + "/.hash")
	index.IndexHash = make(map[[8]byte][4]byte)

	index.IndexInverted = make(map[[8]byte]bool)

	index.TreeIndex = selectIndex(index.Name + "/.tree")

	index.IndexTree = make(map[[8]byte]map[[4]byte][][4]byte)

	index.MemoIndex = selectIndex(index.Name + "/.memo")

	index.LoadHash()
	index.LoadBitmap()
	index.LoadTree()
	index.DictionaryMemo = index.LoadDictionaryMemo()
	index.LoadMemo()

	index.Cache = make(map[[8]byte]struct {
		Val    string
		Expire int
	})

	index.Loading = false
	return index
}
func selectIndex(k string) *Index {
	index := new(Index)
	index.Loading = true
	index.Name = k
	index.File = Config.Storage.Directory + k

	var err error
	index.Config, err = os.OpenFile(index.File+"/.config", os.O_RDONLY, 0600)
	if err != nil {
		log.Fatal(err)
	}
	b2 := make([]byte, 50)
	if _, err = index.Config.Read(b2); err != nil {
		log.Fatal(err)
	}
	index.Config.Close()
	offsetSegL := 0
	for i := 0; i < len(b2); i++ {
		if b2[i] == 0 {
			index.SegL, err = strconv.Atoi(string(b2[offsetSegL:i]))
			break
		}
		if b2[i] == 0x0a {
			index.SegN, err = strconv.Atoi(string(b2[0:i]))
			offsetSegL = i + 1
			continue
		}
	}

	indexes[index.Name] = index
	return index
}

type TagsSortRes struct {
	Results []TagsSortResult
	Size    uint32
}
type TagsSortResult struct {
	Key int
	Val []byte
}

func (index *Index) GetIndex(tag []byte, offset int, limit int, reverse bool, memo int) TagsSortRes {
	if Config.Debug.Log {
		defer StatEnd(StatStart(), "GetIndex", limit)
	}

	var limiter int = 0
	TagsSortRes := TagsSortRes{
		make([]TagsSortResult, limit),
		0,
	}

	tree := index.IndexBitmap[byteToByte8(tag)]

	if tree == nil {
		if Config.Debug.Log {
			log.Println("GetIndex: Out of range")
		}
		return TagsSortRes
	}

	if uint64(offset) > tree.GetCardinality() {
		if Config.Debug.Log {
			log.Println("GetIndex: Out of range")
		}
		return TagsSortRes
	}

	max := uint32(tree.GetCardinality()) //- 1
	TagsSortRes.Size = max
	if reverse {
		for i := max - uint32(offset); i > uint32(0) && limiter < limit; i-- {
			v, _ := tree.Select(uint32(i))
			if v == 0 {
				continue
			}
			if memo == 0 {
				TagsSortRes.Results[limiter] = TagsSortResult{
					int(v),
					index.GetRaw(v),
				}
			} else if memo == 1 {
				TagsSortRes.Results[limiter] = TagsSortResult{
					int(v),
					index.GetMemo(v),
				}
			}
			limiter++
		}
	} else {
		for i := offset; i < offset+limit; i++ {
			v, _ := tree.Select(uint32(i))
			if v == 0 {
				continue
			}
			if memo == 0 {
				TagsSortRes.Results[limiter] = TagsSortResult{
					int(v),
					index.GetRaw(v),
				}
			} else if memo == 1 {
				TagsSortRes.Results[limiter] = TagsSortResult{
					int(v),
					index.GetMemo(v),
				}
			}
			limiter++
		}
	}

	if limiter == 0 {
		if Config.Debug.Log {
			log.Println("GetIndex: Not found")
		}
	}

	//res = res[0:limiter]

	return TagsSortRes
}

func (index *Index) GetIndexCross(tags [][]byte, offset int, limit int, reverse bool, memo int) TagsSortRes {
	if Config.Debug.Log {
		defer StatEnd(StatStart(), "GetIndexCross", limit)
	}

	var limiter int = 0
	TagsSortRes := TagsSortRes{
		make([]TagsSortResult, limit),
		0,
	}
	var tree *roaring.Bitmap

	var treeIntersections []*roaring.Bitmap = make([]*roaring.Bitmap, len(tags))
	var treeIntersection *roaring.Bitmap

	addCount := 0
	for k, t := range tags {
		if len(t) < 2 {
			return TagsSortRes
		}
		// + = byte(0x2B)
		if bytes.Contains(t, []byte{byte(0x7C)}) {
			continue
		}
		// ^ = byte(0x5E)
		if t[0] == byte(0x5E) {
			continue
		}
		treeIntersections[k] = index.IndexBitmap[byteToByte8(t)]
		if treeIntersections[k] == nil {
			if Config.Debug.Log {
				log.Println("GetIndexCross: Out of range")
			}
			return TagsSortRes
		}
		addCount++
	}
	treeIntersections = treeIntersections[0:addCount]

	treeIntersection = roaring.FastAnd(treeIntersections...)

	for _, t := range tags {
		// + = byte(0x2B) / 0x7C = |
		if bytes.Contains(t, []byte{byte(0x7C)}) {
			var treeForAnd *roaring.Bitmap
			treeAnds := bytes.Split(t, []byte("|"))
			for _, v := range treeAnds {
				var x []byte
				x = append(x, v[:]...)
				tree = index.IndexBitmap[byteToByte8(x)]
				if tree != nil {
					if treeForAnd == nil {
						treeForAnd = tree
					} else {
						treeForAnd.Or(tree)
					}
				}
			}
			treeIntersection.And(treeForAnd)
			continue
		}
		// ^ = byte(0x5E)
		if len(t) < 2 {
			continue
		}
		if byte(0x5E) == t[0] {
			var x []byte
			x = append(x, t[1:]...)
			treeXor := index.IndexBitmap[byteToByte8(x)]
			if treeXor != nil {
				treeIntersection.AndNot(treeXor)
			}
		} else {
			continue
		}
	}

	max := uint32(treeIntersection.GetCardinality()) //- 1
	TagsSortRes.Size = max
	if reverse {
		for i := max - uint32(offset); i > uint32(0) && limiter < limit; i-- {
			v, _ := treeIntersection.Select(uint32(i))
			if v == 0 {
				continue
			}
			if memo == 0 {
				TagsSortRes.Results[limiter] = TagsSortResult{
					int(v),
					index.GetRaw(v),
				}
			} else if memo == 1 {
				TagsSortRes.Results[limiter] = TagsSortResult{
					int(v),
					index.GetMemo(v),
				}
			}
			limiter++
		}
	} else {
		for i := offset; i < offset+limit; i++ {
			v, _ := treeIntersection.Select(uint32(i))
			if v == 0 {
				continue
			}
			if memo == 0 {
				TagsSortRes.Results[limiter] = TagsSortResult{
					int(v),
					index.GetRaw(v),
				}
			} else if memo == 1 {
				TagsSortRes.Results[limiter] = TagsSortResult{
					int(v),
					index.GetMemo(v),
				}
			}
			limiter++
		}
	}

	if limiter == 0 {
		if Config.Debug.Log {
			log.Println("GetIndexCross: Not found")
		}
	}

	//res = res[0:limiter]

	return TagsSortRes
}

func (index *Index) LoadIndex() bool {
	godaStat := StatStart()
	defer StatEnd(godaStat, "LoadIndex", index.SegN)

	indexNmax := int(math.Ceil(float64(index.SegN) / float64(index.SegL)))
	for indexN := 1; indexN <= indexNmax; indexN++ {
		f, err := os.OpenFile(index.File+"/.storage/.storage/"+strconv.Itoa(indexN), os.O_RDONLY, 0600)
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

			index.IndexLastID++
			if index.IndexLastID >= indexLen {
				index.Index = append(index.Index, make([][8]byte, 100000)...)
				indexLen += 100000
			}
			index.Index[index.IndexLastID] = byteToByte8(v[4:12])
		}
		if Config.Performance.FreeMemoryOnLoading {
			FreeMemory()
		}
	}

	return true
}
