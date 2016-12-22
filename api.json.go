package main

import (
	"encoding/json"
	//"log"
	//"fmt"
	//"sync"
	"bytes"
	"strconv"
	"strings"
	//"github.com/buger/jsonparser"
)

func ApiStart() {
	//index := SelectIndex("index")
}

/*
	SET /index/
	{"Value":{
		"Data":'{
			"Title":"Full-text search field 0",
			"Text":"Text text text.",
			"Date":"2016-11-27T00:41:18.3439818+05:00",
			"ID":185,
			"Tags":["Tag 429","Tag 963","Tag 822"]
		}',
		"Reserve":1024,
		"Hash":["Full-text search field 0"],
		"Tags":["Tag 429","Tag 963","Tag 822"],
		"Full":[],
		"Tree":{"Data":1480189402085924400}
	}}
*/
type Value struct {
	Data    []byte         // Required
	Memo    []byte         // Optional
	Hash    []string       // Optional
	Tags    []string       // Optional
	Full    []string       // Optional
	Tree    map[string]int // Optional
	Options struct {       // Optional
		Reserve       int // Optional, default 0; Reserved space for this Data in bytes (Required for Update)
		HashDuplicate int // Optional, default 0; 0 - not insert on any duplicate key; 1 - not insert if first key duplicate; 2 - insert duplicate without hash key
	}
}
type SetJsonReq struct {
	Value Value
}
type SetJsonRes struct {
	ID      uint32
	Status  bool
	Results []string
	//Size    int
}

//var mutexAPISetJson = &sync.RWMutex{}

func (index *Index) SetJson(jsonString string) string {
	var jsonReq Value
	var jsonRes SetJsonRes

	err := json.Unmarshal([]byte(jsonString), &jsonReq)
	if err != nil {
		jsonRes.Status = false
		jsonRes.Results = []string{`Wrong request: ` + err.Error()}
		rBinary, _ := json.Marshal(jsonRes)
		return string(rBinary)
	}

	//mutexAPISetJson.Lock()
	key, err := index.Set(jsonReq)
	//mutexAPISetJson.Unlock()

	if err != nil {
		jsonRes.Status = false
		jsonRes.Results = []string{`Not corrrect request: ` + err.Error()}
		rBinary, _ := json.Marshal(jsonRes)
		return string(rBinary)
	}

	jsonRes.ID = byte4UInt32(key) //[]string{fmt.Sprint(byte4UInt32(key))}

	jsonRes.Status = true
	rBinary, _ := json.Marshal(jsonRes)
	return string(rBinary)
}

type GetJsonReq struct {
	Hash string // Optional
	ID   uint32 // Optional
}
type GetJsonRes struct {
	Results []string
	Status  bool
	Size    int
}

func (index *Index) GetJson(jsonString string) string {
	var jsonReq GetJsonReq
	var jsonRes GetJsonRes

	err := json.Unmarshal([]byte(jsonString), &jsonReq)
	if err != nil {
		jsonRes.Status = false
		jsonRes.Results = []string{`Wrong request: ` + err.Error()}
		rBinary, _ := json.Marshal(jsonRes)
		return string(rBinary)
	}

	if jsonReq.Hash == "" && jsonReq.ID == 0 {
		jsonRes.Status = false
		jsonRes.Results = []string{`Need Hash or ID in request.`}
		rBinary, _ := json.Marshal(jsonRes)
		return string(rBinary)
	}

	//mutexAPISetJson.RLock()
	if jsonReq.ID != 0 {
		jsonRes.Results = []string{string(index.GetRaw(jsonReq.ID))}
	}
	if jsonReq.Hash != "" {
		tmp := index.GetByHash([]byte(jsonReq.Hash))
		if tmp != nil {
			jsonRes.Results = []string{string(tmp.Data)}
		}
	}
	//mutexAPISetJson.RUnlock()

	jsonRes.Status = true
	rBinary, _ := json.Marshal(jsonRes)
	return string(rBinary)
}

/*
	GET /index/=Tag+1*Tag+2/0:10
	GET /index/tags/Tag+1,Tag+2/0/0:10/0

	GET /index/=
	{"Tags": ["Tag 1", "Tag 2"],
	"Range": {
		"Offset": 0,
		"Limit": 10
	}}
*/
type TagsSortJsonReq struct {
	Tags  []string
	Range struct {
		Order  string // Optional, default ASC
		Offset int    // Optional, default 0
		Limit  int    // Optional, default 0
	}
	Memo int // Optional, default 0
}
type TagsSortJsonRes struct {
	Results map[uint32]string
	Size    uint32
	Status  bool
}

func (index *Index) TagsSortJson(jsonString string) string {
	//godaStat := StatStart()
	var jsonReq TagsSortJsonReq

	/*
		jsonByte := []byte(jsonString)
		jsonReq.Tags = make([]string, 32)
		i := 0
		jsonparser.ArrayEach(jsonByte, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
			jsonReq.Tags[i] = string(value)
			i++
		}, "Tags")
		jsonReq.Tags = jsonReq.Tags[0:i]
		if value, err := jsonparser.GetString(jsonByte, "Range", "Order"); err == nil {
			jsonReq.Range.Order = string(value)
		} else {
			return `{"Results":{"0":"Wrong request: ` + err.Error() + `"},"Size":0,"Status":false}`
		}
		if value, err := jsonparser.GetInt(jsonByte, "Range", "Offset"); err == nil {
			jsonReq.Range.Offset = int(value)
		} else {
			return `{"Results":{"0":"Wrong request: ` + err.Error() + `"},"Size":0,"Status":false}`
		}
		if value, err := jsonparser.GetInt(jsonByte, "Range", "Limit"); err == nil {
			jsonReq.Range.Limit = int(value)
		} else {
			return `{"Results":{"0":"Wrong request: ` + err.Error() + `"},"Size":0,"Status":false}`
		}
		if value, err := jsonparser.GetInt(jsonByte, "Memo"); err == nil {
			jsonReq.Memo = int(value)
		} else {
			return `{"Results":{"0":"Wrong request: ` + err.Error() + `"},"Size":0,"Status":false}`
		}
	*/

	err := json.Unmarshal([]byte(jsonString), &jsonReq)
	if err != nil {
		/*
			var jsonRes TagsSortJsonRes
			jsonRes.Results = make(map[uint32]string, 1)
			jsonRes.Status = false
			jsonRes.Results[0] = `Wrong request: ` + err.Error()
			rBinary, _ := json.Marshal(jsonRes)
			return string(rBinary)
		*/
		return `{"Results":{"0":"Wrong request: ` + err.Error() + `"},"Size":0,"Status":false}`
	}

	var rByteM TagsSortRes //= make(TagsSortRes, len(jsonReq.Tags))

	var rByteA [][]byte = make([][]byte, len(jsonReq.Tags))
	for k, v := range jsonReq.Tags {
		if len(v) < 2 {
			continue
		}
		if byte(0x5E) == v[0] {
			rByteA[k] = []byte("^t/" + v[1:])
		} else {
			rByteA[k] = []byte("t/" + v)
		}
		if bytes.Contains(rByteA[k], []byte("|")) {
			rByteA[k] = bytes.Replace(rByteA[k], []byte("|"), []byte("|t/"), -1)
			//log.Println(string(rByteA[k]))
		}
	}

	reverse := false
	if jsonReq.Range.Order == "DESC" {
		reverse = true
	}
	//StatEnd(godaStat, "TagsSortJson/ParseReq", 1)
	//godaStat = StatStart()

	//mutexAPISetJson.RLock()
	if len(jsonReq.Tags) == 1 {
		rByteM = index.GetIndex(rByteA[0], jsonReq.Range.Offset, jsonReq.Range.Limit, reverse, jsonReq.Memo)
	} else {
		rByteM = index.GetIndexCross(rByteA, jsonReq.Range.Offset, jsonReq.Range.Limit, reverse, jsonReq.Memo)
	}
	//StatEnd(godaStat, "TagsSortJson/Call", 1)
	//godaStat = StatStart()
	if rByteM.Size == 0 {
		//var jsonRes TagsSortJsonRes
		//jsonRes.Results = make(map[uint32]string)
		//rBinary, _ := json.Marshal(jsonRes)
		//return string(rBinary)
		return `{"Results":{},"Size":0,"Status":false}`
	}
	//mutexAPISetJson.RUnlock()

	/*
		jsonRes.Results = make(map[uint32]string, len(rByteM.Results))

		for k, v := range rByteM.Results {
			jsonRes.Results[k] = string(v)
		}

		jsonRes.Size = rByteM.Size
		jsonRes.Status = true

		rBinary, _ := json.Marshal(jsonRes)
		StatEnd(godaStat, "TagsSortJson/ReturnRes", 1)
		return string(rBinary)
	*/

	// 4.3x better performance then default JSON
	var resultSlice []string = make([]string, len(rByteM.Results))
	//i := 0
	for k, v := range rByteM.Results {
		resultSlice[k] = `"` + strconv.FormatUint(uint64(v.Key), 10) + `":"` + string(v.Val) + `"`
		//i++
	}
	return `{"Results":{` + strings.Join(resultSlice, ",") + `},"Size":` + strconv.FormatUint(uint64(rByteM.Size), 10) + `,"Status":true}`
}

/*
	GET /index/+Date/min:max/ASC/limit
	GET /index/+Date/0:0/ASC/10

	GET /index/+
	{"Sort": {
		"Tree":  "Date",
		"Min":   1480765500,
		"Max":   0,
		"Order": "ASC",
		"Limit": 10
	}}
*/
type TreeSortJsonReq struct {
	Tree string // Required
	Sort struct {
		Min   uint32 // Optional, default 0
		Max   uint32 // Optional, default 0
		Order string // Optional, default ASC
		Limit uint32 // Optional, default 0
	}
	Memo int // Optional, default 0
}
type TreeSortJsonRes struct {
	Results []string
	Size    uint32
	Status  bool
}

func (index *Index) TreeSortJson(jsonString string) string {
	var jsonReq TreeSortJsonReq
	var jsonRes TreeSortJsonRes

	err := json.Unmarshal([]byte(jsonString), &jsonReq)
	if err != nil {
		jsonRes.Status = false
		jsonRes.Results = []string{`Wrong request: ` + err.Error()}
		rBinary, _ := json.Marshal(jsonRes)
		return string(rBinary)
	}

	treeName := []byte(jsonReq.Tree)

	reverse := false
	if jsonReq.Sort.Order == "DESC" {
		reverse = true
	}

	//mutexAPISetJson.RLock()
	rByteA := index.TreeSort(treeName, jsonReq.Sort.Min, jsonReq.Sort.Max, jsonReq.Sort.Limit, reverse, jsonReq.Memo)
	//mutexAPISetJson.RUnlock()

	if index.IndexTree[byteToByte8(treeName)] == nil {
		jsonRes.Status = false
		jsonRes.Results = []string{`Tree not found.`}
		rBinary, _ := json.Marshal(jsonRes)
		return string(rBinary)
	}

	jsonRes.Results = make([]string, len(rByteA.Results))

	for k, v := range rByteA.Results {
		jsonRes.Results[k] = string(v)
	}

	jsonRes.Size = rByteA.Size
	jsonRes.Status = true

	rBinary, _ := json.Marshal(jsonRes)
	return string(rBinary)
}

// Cache

type CacheGetJsonReq struct {
	Key string // Required
}
type CacheGetJsonRes struct {
	Results []string
	Status  bool
	Size    int
}

func (index *Index) CacheGetJson(jsonString string) string {
	var jsonReq CacheGetJsonReq
	var jsonRes CacheGetJsonRes

	err := json.Unmarshal([]byte(jsonString), &jsonReq)
	if err != nil {
		jsonRes.Status = false
		jsonRes.Results = []string{`Wrong request: ` + err.Error()}
		rBinary, _ := json.Marshal(jsonRes)
		return string(rBinary)
	}

	if jsonReq.Key == "" {
		jsonRes.Status = false
		jsonRes.Results = []string{`Need Key in request.`}
		rBinary, _ := json.Marshal(jsonRes)
		return string(rBinary)
	}

	tmp, err := index.CacheGet(jsonReq.Key)
	if err != nil {
		jsonRes.Status = false
		jsonRes.Results = []string{`Error: ` + err.Error()}
		rBinary, _ := json.Marshal(jsonRes)
		return string(rBinary)
	} else {
		jsonRes.Results = []string{tmp}
	}

	jsonRes.Status = true
	rBinary, _ := json.Marshal(jsonRes)
	return string(rBinary)
}

type CacheSetJsonReq struct {
	Key    string // Required
	Value  string // Required
	Expire int    // Required
}
type CacheSetJsonRes struct {
	Results []string
	Status  bool
	Size    int
}

func (index *Index) CacheSetJson(jsonString string) string {
	var jsonReq CacheSetJsonReq
	var jsonRes CacheSetJsonRes

	err := json.Unmarshal([]byte(jsonString), &jsonReq)
	if err != nil {
		jsonRes.Status = false
		jsonRes.Results = []string{`Wrong request: ` + err.Error()}
		rBinary, _ := json.Marshal(jsonRes)
		return string(rBinary)
	}

	if jsonReq.Key == "" || jsonReq.Value == "" || jsonReq.Expire <= 0 {
		jsonRes.Status = false
		jsonRes.Results = []string{`Need Key, Value and Expire in request.`}
		rBinary, _ := json.Marshal(jsonRes)
		return string(rBinary)
	}

	tmp := index.CacheSet(jsonReq.Key, jsonReq.Value, jsonReq.Expire)
	if !tmp {
		jsonRes.Status = false
		jsonRes.Results = []string{`Error: ` + err.Error()}
		rBinary, _ := json.Marshal(jsonRes)
		return string(rBinary)
	}

	jsonRes.Status = true
	rBinary, _ := json.Marshal(jsonRes)
	return string(rBinary)
}
