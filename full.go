package main

import (
	"strings"
)

var replacerSymbols = strings.NewReplacer(
	"▾", " ",
	"—", "-",
	"_", " ",
	"─", " ",
	",", " ",
	"·", " ",
	".", " ",
	"!", " ",
	"?", " ",
	":", " ",
	";", " ",
	"{", " ",
	"}", " ",
	"[", " ",
	"]", " ",
	"(", " ",
	")", " ",
	"=", " ",
	"+", " ",
	"@", " ",
	"#", " ",
	"№", " ",
	"$", " ",
	"%", " ",
	"^", " ",
	"&", " ",
	"*", " ",
	"~", " ",
	"`", " ",
	"'", " ",
	"\"", " ",
	"\\", " ",
	"/", " ",
	"|", " ",
	"\t", " ",
	"\r\n", " ",
	"\n", " ",
	"<", "",
	">", "")
var replacerSymbols2 = strings.NewReplacer(
	" -", " ",
	"- ", " ",
	"--", " ")
var replacerTrim = strings.NewReplacer(
	"    ", " ",
	"   ", " ",
	"  ", " ")

func (index Index) InvertedIndex(key []byte, s string) {
	s = replacerSymbols.Replace(s)
	s = replacerSymbols2.Replace(s)
	s = replacerTrim.Replace(s)
	s = replacerTrim.Replace(s)
	s = replacerTrim.Replace(s)
	s = strings.ToLower(s)

	sA := strings.Split(s, " ")

	for _, v := range sA {
		var vByte []byte = []byte(v)[:8]
		var vByte8 [8]byte = byteToByte8(vByte)

		if !index.IndexInverted[vByte8] {
			index.IndexInverted[vByte8] = true
			index.BitmapAdd(append([]byte("s/"), vByte...), byte4UInt32(key))
		}
	}
}
func (index Index) Search(q string) {
	var vByte [8]byte
	var x []byte = []byte(q)
	copy(vByte[:], x[:8])
	if index.IndexInverted[vByte] {
		var bitmapName []byte = x[:8]
		index.BitmapRead(append([]byte("s/"), bitmapName...))
	}
}
