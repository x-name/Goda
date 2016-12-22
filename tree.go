package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"math"
	"os"
	//"runtime"
	"github.com/RoaringBitmap/roaring"
	"strconv"
	//"time"
	//"github.com/emirpasic/gods/maps/treemap"
	//"sync"
)

func (index *Index) TreeRead() {
	godaStat := StatStart()
	//return
	tree := index.IndexBitmap[byteToByte8([]byte("t/Tag 1"))]
	i := tree.Iterator()
	for i.HasNext() {
		//if asdfs > 1000000 {
		//	break
		//}
		i.Next()
		//v := i.Next()
		//log.Println(v)

		//_ = index.IndexTree[byteToByte4(uInt32Byte4(v))]

		//log.Println(ids)
		//log.Panic(len(ids))
		//log.Println(i.Next())
	}
	log.Println("Cardinality: ", tree.GetCardinality())
	log.Println("Contains 3? ", tree.Contains(3))
	// smaller or equal to x (Rank(infinity) would be GetCardinality())
	log.Println("Rank", tree.Rank(5000000))
	_, v := tree.Select(1113496)
	log.Println("Select", v)

	godaStat = StatEnd(godaStat, "TreeRead", index.SegN)

	treeIntersection := roaring.And(index.IndexBitmap[byteToByte8([]byte("t/Tag 1"))], index.IndexBitmap[byteToByte8([]byte("t/Tag 2"))])
	i = treeIntersection.Iterator()
	for i.HasNext() {
		i.Next()
	}
	log.Println("Cardinality: ", treeIntersection.GetCardinality())
	log.Println("Contains 3? ", treeIntersection.Contains(3))
	// smaller or equal to x (Rank(infinity) would be GetCardinality())
	log.Println("Rank", treeIntersection.Rank(5000000))

	godaStat = StatEnd(godaStat, "TreeRead Intersection 2", index.SegN)

	treeIntersection = roaring.And(index.IndexBitmap[byteToByte8([]byte("t/Tag 1"))], index.IndexBitmap[byteToByte8([]byte("t/Tag 2"))])
	treeIntersection = roaring.And(treeIntersection, index.IndexBitmap[byteToByte8([]byte("t/Tag 3"))])
	i = treeIntersection.Iterator()
	for i.HasNext() {
		i.Next()
	}
	log.Println("Cardinality: ", treeIntersection.GetCardinality())
	log.Println("Contains 3? ", treeIntersection.Contains(3))
	// smaller or equal to x (Rank(infinity) would be GetCardinality())
	log.Println("Rank", treeIntersection.Rank(5000000))

	godaStat = StatEnd(godaStat, "TreeRead Intersection 3", index.SegN)
}

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

	//firstMax := uint32(tree.GetCardinality()) - 1
	if minVal > 0 {
		min, _ := tree.Select(uint32(0))
		tree.RemoveRange(uint64(min), uint64(minVal))
	}

	//secondMax := uint32(tree.GetCardinality()) - 1
	if maxVal > 0 {
		max, _ := tree.Select(uint32(tree.GetCardinality() - 1))
		tree.RemoveRange(uint64(maxVal), uint64(max))
	}

	max := uint32(tree.GetCardinality()) - 1
	//log.Println(tree.GetCardinality())
	//log.Println(max)
	//log.Println(firstMax - max)
	//log.Println(secondMax - max)
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

	/*
		for i := offset; i < offset+limit; i++ {
			v, _ := tree.Select(uint32(i))
			res[limiter] = index.GetRaw(v)
			limiter++
		}
	*/
	treeOrder := roaring.New()

	for i := uint32(0); i < max && limiter < limit; i++ {
		v, _ := tree.Select(uint32(i))
		vs := index.IndexTree[nameByte8][byteToByte4(uInt32Byte4(v))]
		//log.Println(vs)
		for _, v2 := range vs {
			//if limiter >= limit {
			//	break
			//}
			//res[limiter] = index.GetRaw(byte4UInt32(v2[:]))
			treeOrder.Add(byte4UInt32(v2[:]))
			limiter = uint32(treeTag.AndCardinality(treeOrder))
			//limiter = 0
			//log.Println(limiter)
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

/*
func (index *Index) TreeSortOld(name string, minVal int, maxVal int, minPos int, limit int, reverse bool) [][]byte {
	start := time.Now()

	it := index.Tree[name].Iterator()
	i := 0
	var limiter int = 0

	var r [][]byte = make([][]byte, limit)

	if reverse {
		for it.End(); it.Prev(); {
			i++
			key, value := it.Key(), it.Value()

			if limiter == limit || (maxVal > 0 && key.(int) <= maxVal) {
				log.Printf("Tree Listing (DESC): %s; %d returned; scanned %d from %d\n", time.Since(start), limiter, i, index.Tree["Data"].Size())
				break
			}

			if (minVal > 0 && key.(int) < minVal) || (minPos > 0 && i > minPos) || (minVal == 0 && minPos == 0) {
				//r[limiter] = index.GetRaw(UnEscape(value.(string)))
				limiter++
			}
		}
	} else {
		for it.Next() {
			i++
			key, value := it.Key(), it.Value()

			if limiter == limit || (maxVal > 0 && key.(int) >= maxVal) {
				log.Printf("Tree Listing (ASC): %s; %d returned; scanned %d from %d\n", time.Since(start), limiter, i, index.Tree["Data"].Size())
				break
			}

			if (minVal > 0 && key.(int) > minVal) || (minPos > 0 && i > minPos) || (minVal == 0 && minPos == 0) {
				//r[limiter] = index.GetRaw(UnEscape(value.(string)))
				limiter++
			}
		}
	}

	log.Printf("Tree END: %s\n", time.Since(start))

	return r[0:limiter]
}
*/

func (index *Index) LoadTree() bool {
	godaStat := StatStart()
	defer StatEnd(godaStat, "LoadTree", index.TreeIndex.SegN)

	//index.IndexTree = make(map[[4]byte][][4]byte, index.TreeIndex.SegN)

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
		//log.Println(len(strokes))
		//godaStat = StatStart()
		for _, v := range strokes {
			if len(v) <= 5 {
				continue
			}
			size := binary.LittleEndian.Uint32(v[0:4])
			if uint32(len(v[4:])) != size {
				continue
			}
			vSplit := bytes.Split(v[4:], bDelimeter2)

			/*
				k := string(vSplit[0])
				if index.Tree[k] == nil {
					index.Tree[k] = treemap.NewWithIntComparator()
				}
				index.Tree[k].Put(TreeOrderCRC(int(byte8UInt64(vSplit[1])), vSplit[2]), Escape(vSplit[2]))
			*/

			indexTreeMapName := byteToByte8(vSplit[0])
			v := byte8UInt64(vSplit[1])
			key := vSplit[2]

			//BitmapAdd([]byte("t/"+string(vSplit[0])), uint32(v))
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
		//godaStat = StatEnd(godaStat)
		if Config.Performance.FreeMemoryOnLoading {
			FreeMemory()
		}
	}
	//log.Println(index.IndexTree)
	/*
		treeLen := 0
		if len(index.IndexTree) > 0 {
			for _, t := range index.IndexTree {
				treeLen += len(t)
			}
		}
	*/

	return true
}
