package main

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"encoding/hex"
	"io"
	"log"
	"math"
	"runtime"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/kjk/smaz"

	"bufio"
	//"fmt"
	"os"
<<<<<<< HEAD
	//"sync"
	"sync"
	//"sync/atomic"
=======
	"sync"
>>>>>>> parent of 0136da4... tags limit bugfix
)

//var bDelimeter []byte = []byte{0x00, 0xBC}

var bDelimeter []byte = []byte{0x00, 0x00, 0x00, 0x00, 0xBC, 0xBC, 0xBC, 0xBC}
var bDelimeter2 []byte = []byte{0x00, 0x00, 0x01, 0x01, 0xBC, 0xBC}
var b4null []byte = []byte{0x00, 0x00, 0x00, 0x00}

//var WriteBufferFiles = 100
//var WriteBufferLength = 1024 * 10240 // 1MB+
//var WriteBufferWaiting = time.Duration(10)

var writeBuffer map[string][]byte
<<<<<<< HEAD

=======
>>>>>>> parent of 0136da4... tags limit bugfix
var mutex = &sync.RWMutex{}
var lastWriterFile *os.File
var lastWriterFileName string

func Writer() {
	var err error
	mutex.Lock()
	for fileName := range writeBuffer {
		if lastWriterFile == nil || lastWriterFileName != fileName {
			lastWriterFileName = fileName
			lastWriterFile, err = os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				lastWriterFile = nil
				log.Println(err)
			}
		}
		w := bufio.NewWriter(lastWriterFile)

		w.Write(writeBuffer[fileName])
		delete(writeBuffer, fileName)
		//writeBuffer[fileName] = nil

		err = w.Flush()
		if err != nil {
			log.Println(err)
		}
	}
	mutex.Unlock()
}
func WriteAppend(fileName string, b []byte) bool {
<<<<<<< HEAD
	//mutex.Lock()
	//bLen := len(b)

=======
>>>>>>> parent of 0136da4... tags limit bugfix
	mutex.Lock()
	if writeBuffer == nil {
		// writeBuffer = make(map[string][]byte), WriteBufferFiles) // BAD, slower (go1.7, windows/amd64)
		writeBuffer = make(map[string][]byte)
	}
	if writeBuffer[fileName] == nil {
		// writeBuffer[fileName] = make([]byte, WriteBufferLength) // BAD, slower (go1.7, windows/amd64)
		writeBuffer[fileName] = []byte{}
	}

	/*
		// 30% CPU load after work if this call from gorutine
		// if - may provoke race condition
		// 1 Lock to 2 RLock - BAD, slower (go1.7, windows/amd64)
		if len(writeBuffer) >= WriteBufferFiles {
			mutex.Unlock()
			t := time.NewTicker(WriteBufferWaiting * time.Millisecond)
			for range t.C {
				mutex.RLock()
				if len(writeBuffer) < WriteBufferFiles {
					mutex.RUnlock()
					break
				}
				mutex.RUnlock()
			}
			mutex.Lock()
		}
		if len(writeBuffer[fileName])+bLen >= WriteBufferLength {
			mutex.Unlock()
			t := time.NewTicker(WriteBufferWaiting * time.Millisecond)
			for range t.C {
				mutex.RLock()
				if len(writeBuffer[fileName])+bLen < WriteBufferLength {
					mutex.RUnlock()
					break
				}
				mutex.RUnlock()
			}
			mutex.Lock()
		}
	*/

	writeBuffer[fileName] = append(writeBuffer[fileName], b...)
	mutex.Unlock()

	return true
}

var writerTruncateBuffer map[string][]byte = make(map[string][]byte)
<<<<<<< HEAD

=======
>>>>>>> parent of 0136da4... tags limit bugfix
var mutexWriterTruncate = &sync.RWMutex{}
var lastWriterTruncateFile *os.File
var lastWriterTruncateFileName string

func WriterTruncate() {
	var err error
	mutexWriterTruncate.Lock()
	for fileName := range writerTruncateBuffer {
		if lastWriterTruncateFileName != fileName {
			lastWriterTruncateFileName = fileName
			lastWriterTruncateFile, err = os.OpenFile(fileName, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
			if err != nil {
				lastWriterTruncateFile = nil
				lastWriterTruncateFileName = ""
				log.Println(err)
			}
		}
		_, err := lastWriterTruncateFile.WriteAt(writerTruncateBuffer[fileName], 0)
		if err != nil {
			lastWriterTruncateFile = nil
			lastWriterTruncateFileName = ""
			log.Println(err)
		} else {
			delete(writerTruncateBuffer, fileName)
		}
	}
	mutexWriterTruncate.Unlock()
}
func WriteTruncate(fileName string, b []byte) bool {
<<<<<<< HEAD
	//mutexWriterTruncate.Lock()
	mutexWriterTruncate.Lock()
	//if writerTruncateBuffer == nil {
	//	writerTruncateBuffer
	//}
	//if writerTruncateBuffer[fileName] == nil {
	//	writerTruncateBuffer[fileName] = []byte{}
	//}
=======
	mutexWriterTruncate.Lock()
>>>>>>> parent of 0136da4... tags limit bugfix
	writerTruncateBuffer[fileName] = b
	mutexWriterTruncate.Unlock()

	return true
}

func Decode(buff []byte) []byte {
	//return buff
	method := buff[0]
	buff = buff[1:]
	if method == 0x01 {
		r, err := smaz.Decode(nil, buff)
		//log.Println("Decode:")
		//log.Println(buff)
		if err != nil {
			log.Println(err)
		}
		return []byte(r)
	} else {
		return buff

		var out bytes.Buffer
		b := bytes.NewReader(buff)
		r, _ := zlib.NewReader(b)
		//r := snappy.NewReader(b)
		r.Close()
		io.Copy(&out, r)
		return out.Bytes()
	}
}
func Encode(buff []byte) []byte {
	//return buff
	//l := len(buff)
	if true {
		r := append([]byte{0x01}, smaz.Encode(nil, buff)...)
		//log.Println("Encode:")
		//log.Println(r)
		return r
	} else {
		var b bytes.Buffer
		w, _ := zlib.NewWriterLevel(&b, 9)
		//w := snappy.NewWriter(&b)
		w.Write(buff)
		w.Close()
		return append([]byte{0x02}, b.Bytes()...)
	}
}

func Escape(k []byte) string {
	return hex.EncodeToString(k)
}
func UnEscape(k string) []byte {
	r, _ := hex.DecodeString(k)
	return r
}

func uInt32Byte4(v uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(v))
	return b
}
func byte4UInt32(v []byte) uint32 {
	return binary.LittleEndian.Uint32(v)
}
func uInt64Byte8(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}
func byte8UInt64(v []byte) uint64 {
	return binary.BigEndian.Uint64(v)
}

func byteToByte4(b []byte) [4]byte {
	r := [4]byte{}
	copy(r[:], b)
	return r
}
func byteToByte8(b []byte) [8]byte {
	r := [8]byte{}
	copy(r[:], b)
	return r
}

func TreeOrderCRC(sort int, b []byte) int {
	minVal := 1000000000000000000
	if sort/minVal < 1 {
		sort += minVal
	}
	for _, crc := range b {
		sort += int(crc)
	}
	return sort
}

func FormatSize(i uint64) string {
	var r string

	/*if i > 1073741824 {
		r = strconv.FormatUint(i/1024/1024/1024/1024/1024/1024, 10) + "GB"
	} else*/if i > 1048576 {
		r = strconv.FormatUint(i/1048576, 10) + "MB"
	} else if i > 1024 {
		r = strconv.FormatUint(i/1024, 10) + "KB"
	} else {
		r = strconv.FormatUint(i, 10) + "B"
	}
	return r
}

func AppendByte(slice []byte, data ...byte) []byte {
	m := len(slice)
	n := m + len(data)
	if n > cap(slice) {
		a := (n + 1) + int(math.Ceil(float64(n)/20))
		newSlice := make([]byte, a)
		copy(newSlice, slice)
		slice = newSlice
	}
	slice = slice[0:n]
	copy(slice[m:n], data)
	return slice
}

type Stat struct {
	Time   time.Time
	Memory runtime.MemStats
}

/*
var godaStat Stat
godaStat = StatStart()
godaStat = StatEnd(godaStat, "FuncName", 10000000)
*/
func FreeMemory() {
	debug.FreeOSMemory()
}
func StatStart() Stat {
	// More control for tests, if GC work automatic - maybe negative memory results
	//runtime.GC()
	//debug.FreeOSMemory()

	var Memory runtime.MemStats
	runtime.ReadMemStats(&Memory)
	return Stat{
		time.Now(),
		Memory,
	}
}
func StatEnd(stat Stat, s string, count int) Stat {
	// More control for tests, if GC work automatic - maybe negative memory results
	//runtime.GC()
	//debug.FreeOSMemory()

	var Memory runtime.MemStats
	runtime.ReadMemStats(&Memory)
	statNow := Stat{
		time.Now(),
		Memory,
	}

	//logStatMemory(s+"[TA]", count, stat.Time, stat.Memory.TotalAlloc, statNow.Memory.TotalAlloc)
	logStatMemory(s+"[A]", count, stat.Time, stat.Memory.Alloc, statNow.Memory.Alloc)

	return statNow
}
func getStat(s string, countK int, start time.Time, alloc uint64) {
	logStatMemory(s, countK, start, alloc, readMemStats())
}
func readMemStats() uint64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.Alloc
}
func timeNanoFormat(t int64) string {
	s := ""
	if t > time.Second.Nanoseconds() {
		s = strconv.FormatInt(t/time.Second.Nanoseconds(), 10) + "s"
	} else if t > time.Millisecond.Nanoseconds() {
		s = strconv.FormatInt(t/time.Millisecond.Nanoseconds(), 10) + "ms"
	} else if t > time.Microsecond.Nanoseconds() {
		//s = strconv.FormatInt(t/time.Microsecond.Nanoseconds(), 10) + "µs"
		s = strconv.FormatInt(t/time.Microsecond.Nanoseconds()*1000, 10)
	} else {
		s = strconv.FormatFloat(float64(t), 'E', -1, 64)
	}
	//s += strconv.FormatInt(d.Nanoseconds(), 10)
	return s
}
func logStatMemory(s string, count int, start time.Time, alloc uint64, alloc2 uint64) {
	if !Config.Debug.Log {
		return
	}
	if count > 0 {
		log.Printf("%s: %s // Memory: %s; %d; %d bytes/entry\n",
			s,
			//timeNanoFormat(time.Now().UnixNano()-start.UnixNano()),
			time.Since(start),
			FormatSize(alloc2-alloc),
			count,
			(alloc2-alloc)/uint64(count))
	}
}
func logStat(s string, count int, fileCount int, start time.Time, fileSize int64, alloc uint64, alloc2 uint64) {
	if !Config.Debug.Log {
		return
	}
	if count > 0 {
		log.Printf("%s: %s // File: %s; %d // Memory: %s; %d; %d bytes/entry\n",
			s,
			time.Since(start),
			FormatSize(uint64(fileSize)),
			fileCount,
			//FormatSize(int(alloc2-alloc)),
			FormatSize(alloc2-alloc),
			count,
			(alloc2-alloc)/uint64(count))
	}
}
