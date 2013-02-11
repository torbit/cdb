package cdb

import (
	"bytes"
	"io/ioutil"
	"launchpad.net/gommap"
	"math/rand"
	"os"
	"testing"
)

var benchRecords []rec
var benchRecordsBytes []byte
var benchRecordsKeys [][]byte

func init() {
	rng := rand.New(rand.NewSource(0))
	for i := 0; i < 1000; i++ {
		key := make([]byte, rng.Intn(30)+5)
		val := make([]byte, rng.Intn(300)+10)
		for j := 0; j < len(key); j++ {
			key[j] = byte(rng.Int())
		}
		for j := 0; j < len(val); j++ {
			val[j] = byte(rng.Uint32())
		}
		benchRecordsKeys = append(benchRecordsKeys, key)
		benchRecords = append(benchRecords, rec{string(key), []string{string(val)}})
	}
	benchRecordsBytes = newDBBytes(benchRecords)
}

func BenchmarkMemBytes(b *testing.B) {
	benchBytes(b, New(bytes.NewReader(benchRecordsBytes)))
}
func BenchmarkMemReader(b *testing.B) {
	benchReader(b, New(bytes.NewReader(benchRecordsBytes)))
}
func BenchmarkDiskBytes(b *testing.B) {
	file := createDBFile()
	defer os.Remove(file.Name())
	defer file.Close()

	benchBytes(b, New(file))
}
func BenchmarkDiskReader(b *testing.B) {
	file := createDBFile()
	defer os.Remove(file.Name())
	defer file.Close()

	benchReader(b, New(file))
}
func BenchmarkMmapBytes(b *testing.B) {
	file := createDBFile()
	defer os.Remove(file.Name())
	defer file.Close()
	m, err := gommap.Map(file.Fd(), gommap.PROT_READ, gommap.MAP_SHARED)
	if err != nil {
		b.Fatal(err)
	}
	defer m.UnsafeUnmap()

	benchBytes(b, New(bytes.NewReader(m)))
}
func BenchmarkMmapReader(b *testing.B) {
	file := createDBFile()
	defer os.Remove(file.Name())
	defer file.Close()
	m, err := gommap.Map(file.Fd(), gommap.PROT_READ, gommap.MAP_SHARED)
	if err != nil {
		b.Fatal(err)
	}
	defer m.UnsafeUnmap()

	benchReader(b, New(bytes.NewReader(m)))
}

func benchBytes(b *testing.B, db *Cdb) {
	rng := rand.New(rand.NewSource(0))
	numKeys := len(benchRecordsKeys)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = db.Bytes(benchRecordsKeys[rng.Intn(numKeys)])
	}
}
func benchReader(b *testing.B, db *Cdb) {
	rng := rand.New(rand.NewSource(0))
	numKeys := len(benchRecordsKeys)
	var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ := db.Reader(benchRecordsKeys[rng.Intn(numKeys)])
		buf.Reset()
		buf.ReadFrom(r)
	}
}

func createDBFile() *os.File {
	file, err := ioutil.TempFile("", "")
	if err != nil {
		panic(err)
	}
	_, err = file.Write(benchRecordsBytes)
	if err != nil {
		file.Close()
		os.Remove(file.Name())
		panic(err)
	}
	return file
}
