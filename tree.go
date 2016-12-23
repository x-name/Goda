package main

import (
	"bytes"
	"encoding/binary"
	"github.com/RoaringBitmap/roaring"
	"log"
	"math"
	"os"
	"strconv"
)

type TreeSortRes struct {
	Results [][]byte
	Size    uint32
}

func (index *Index) TreeSort(name []byte, minVal uint32, maxVal uint32, limit uint32, reverse bool, memo int) TreeSortRes {
	godaStat := StatStart()

	var limiter uint32 = 0

	treeSortRes := TreeSortRes{
		make([][]byte, limit),
		0,
	}
	var res [][]byte = make([][]byte, limit)

	tree := index.IndexBitmap[byteToByte8(append([]byte("o/"), name...))]
	if tree == nil {
		if Config.Debug.Log {
			log.Println("TreeSort: Not found")
		}
		return treeSortRes
	}
	tree = tree.Clone()

	if minVal > 0 {
		min, _ := tree.Select(uint32(0))
		tree.RemoveRange(uint64(min), uint64(minVal))
	}

	if maxVal > 0 {
		max, _ := tree.Select(uint32(tree.GetCardinality() - 1))
		tree.RemoveRange(uint64(maxVal), uint64(max))
	}

	max := uint32(tree.GetCardinality()) - 1
	if max == 0 {
		if Config.Debug.Log {
			log.Println("TreeSort: Not found")
		}
		return treeSortRes
	}

	nameByte8 := byteToByte8(name)
	if reverse {
		for i := max; (i >= uint32(0) && limit > 0 && limiter < limit) || (i >= uint32(0) && limit == 0); i-- {
			v, _ := tree.Select(uint32(i))
			vs := index.IndexTree[nameByte8][byteToByte4(uInt32Byte4(v))]
			treeSortRes.Size += uint32(len(vs))
			for i2 := len(vs) - 1; i2 >= 0; i2-- {
				if limiter >= limit {
					break
				}
				if memo == 0 {
					res[limiter] = index.GetRaw(byte4UInt32(vs[i2][:]))
				} else if memo == 1 {
					res[limiter] = index.GetMemo(byte4UInt32(vs[i2][:]))
				}
				limiter++
			}
		}
	} else {
		for i := uint32(0); (i < max && limit > 0 && limiter < limit) || (i < max && limit == 0); i++ {
			v, _ := tree.Select(uint32(i))
			vs := index.IndexTree[nameByte8][byteToByte4(uInt32Byte4(v))]
			treeSortRes.Size += uint32(len(vs))
			for _, v2 := range vs {
				if limiter >= limit {
					break
				}
				if memo == 0 {
					res[limiter] = index.GetRaw(byte4UInt32(v2[:]))
				} else if memo == 1 {
					res[limiter] = index.GetMemo(byte4UInt32(v2[:]))
				}
				limiter++
			}
		}
	}

	if limiter == 0 {
		if Config.Debug.Log {
			log.Println("TreeSort: Not found")
		}
	}

	treeSortRes.Results = res[0:limiter]

	defer StatEnd(godaStat, "TreeSort", int(limiter))
	return treeSortRes
}

func (index *Index) TagsTreeSort(tag []byte, name []byte, minVal uint32, maxVal uint32, limit uint32, reverse bool) [][]byte {
	godaStat := StatStart()

	var limiter uint32 = 0
	var res [][]byte = make([][]byte, limit)

	tree := index.IndexBitmap[byteToByte8(append([]byte("o/"), name...))].Clone()

	if minVal > 0 {
		min, _ := tree.Select(uint32(0))
		tree.RemoveRange(uint64(min), uint64(minVal))
	}

	if maxVal > 0 {
		max, _ := tree.Select(uint32(tree.GetCardinality() - 1))
		tree.RemoveRange(uint64(maxVal), uint64(max))
	}

	max := uint32(tree.GetCardinality()) - 1
	if max == 0 {
		log.Println("TagsTreeSort: Not found")
		return res
	}

	nameByte8 := byteToByte8(name)

	treeTag := index.IndexBitmap[byteToByte8(tag)]

	treeOrder := roaring.New()

	for i := uint32(0); i < max && limiter < limit; i++ {
		v, _ := tree.Select(uint32(i))
		vs := index.IndexTree[nameByte8][byteToByte4(uInt32Byte4(v))]
		for _, v2 := range vs {
			treeOrder.Add(byte4UInt32(v2[:]))
			limiter = uint32(treeTag.AndCardinality(treeOrder))
			if limiter == limit {
				break
			}
		}
	}

	treeRes := roaring.And(treeTag, treeOrder)

	limiter = 0
	i := treeRes.Iterator()
	for i.HasNext() {
		if limiter < 100 {
			res[limiter] = index.GetRaw(i.Next())
		}
		limiter++
		if limiter >= limit {
			break
		}
	}

	if limiter == 0 {
		log.Println("TagsTreeSort: Not found")
	}

	res = res[0:limiter]

	defer StatEnd(godaStat, "TagsTreeSort", int(limiter))
	return res
}

func (index *Index) LoadTree() bool {
	godaStat := StatStart()
	defer StatEnd(godaStat, "LoadTree", index.TreeIndex.SegN)

	indexNmax := int(math.Ceil(float64(index.TreeIndex.SegN) / float64(index.TreeIndex.SegL)))
	for indexN := 1; indexN <= indexNmax; indexN++ {

		f, err := os.OpenFile(index.File+"/.tree/.storage/"+strconv.Itoa(indexN), os.O_RDONLY, 0600)
		if err != nil {
			log.Fatal(err)
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
			vSplit := bytes.Split(v[4:], bDelimeter2)

			indexTreeMapName := byteToByte8(vSplit[0])
			v := byte8UInt64(vSplit[1])
			key := vSplit[2]

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

		if Config.Performance.FreeMemoryOnLoading {
			FreeMemory()
		}
	}

	return true
}
