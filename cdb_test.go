package cdb

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

type rec struct {
	key    string
	values []string
}

var records = []rec{
	{"one", []string{"1"}},
	{"two", []string{"2", "22"}},
	{"three", []string{"3", "33", "333"}},
}

var data []byte // set by init()

func TestCdb(t *testing.T) {
	tmp, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatalf("Failed to create temp file: %s", err)
	}

	defer os.Remove(tmp.Name())

	// Test Make
	err = Make(tmp, bytes.NewBuffer(data))
	if err != nil {
		t.Fatalf("Make failed: %s", err)
	}

	// Test reading records
	c, err := Open(tmp.Name())
	if err != nil {
		t.Fatalf("Error opening %s: %s", tmp.Name(), err)
	}
	for _, rec := range records {
		key := []byte(rec.key)
		values := rec.values

		v, err := c.Bytes(key)
		if err != nil {
			t.Fatalf("Record read failed: %s", err)
		}

		if !bytes.Equal(v, []byte(values[0])) {
			t.Fatal("Incorrect value returned")
		}

		iter := c.Iterate(key)
		for _, value := range values {
			sr, err := iter.NextReader()
			if err != nil {
				t.Fatalf("Record read failed: %s", err)
			}

			data := make([]byte, sr.Size())
			_, err = sr.Read(data)
			if err != nil {
				t.Fatalf("Record read failed: %s", err)
			}

			if !bytes.Equal(data, []byte(value)) {
				t.Fatal("value mismatch")
			}
		}
		// Read all values, so should get EOF
		_, err = iter.NextReader()
		if err != io.EOF {
			t.Fatalf("Expected EOF, got %s", err)
		}
	}

	// Test Dump
	if _, err = tmp.Seek(0, 0); err != nil {
		t.Fatal(err)
	}

	buf := bytes.NewBuffer(nil)
	err = Dump(buf, tmp)
	if err != nil {
		t.Fatalf("Dump failed: %s", err)
	}

	if !bytes.Equal(buf.Bytes(), data) {
		t.Fatalf("Dump round-trip failed")
	}
}

func TestNotFound(t *testing.T) {
	db := newDB(records)
	b, err := db.Bytes([]byte("asdf"))
	if err != io.EOF {
		t.Errorf("err: expected EOF, got: %v", err)
	}
	if b != nil {
		t.Errorf("b: expected nil, got: %s", b)
	}
}

func TestBytes(t *testing.T) {
	db := newDB(records)
	b, err := db.Bytes([]byte("one"))
	if err != nil {
		t.Errorf("Bytes error: %v", err)
	}
	if !bytes.Equal(b, []byte("1")) {
		t.Errorf("b: expected 1, got: %s", b)
	}
}

func TestReader(t *testing.T) {
	db := newDB(records)
	r, err := db.Reader([]byte("one"))
	if err != nil {
		t.Errorf("Reader error: %v", err)
	}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		t.Errorf("ReadAll error: %v", err)
	}
	if !bytes.Equal(b, []byte("1")) {
		t.Errorf("b: expected 1, got: %s", b)
	}
}

func TestForEach(t *testing.T) {
	type TestCase struct {
		Key, Val string
	}
	cases := []TestCase{
		{"one", "1"},
		{"two", "2"},
		{"two", "22"},
		{"three", "3"},
		{"three", "33"},
		{"three", "333"},
	}
	db := newDB(records)
	i := 0
	err := db.ForEachBytes(func(key, val []byte) error {
		defer func() { i++ }()
		if i >= len(cases) {
			return nil
		}
		if !bytes.Equal(key, []byte(cases[i].Key)) {
			t.Errorf("case %v: expected key %s, got: %s", i, cases[i].Key, key)
		}
		if !bytes.Equal(val, []byte(cases[i].Val)) {
			t.Errorf("case %v: expected val %s, got: %s", i, cases[i].Val, val)
		}
		return nil
	})
	if err != nil {
		t.Errorf("ForEach error: %v", err)
	}
	if i != len(cases) {
		t.Errorf("ForEach callback called the wrong number of times: expected %v, got: %v", len(cases), i)
	}
}

func newDBBytes(recs []rec) []byte {
	tmp, err := ioutil.TempFile("", "")
	if err != nil {
		panic(err)
	}
	defer os.Remove(tmp.Name())
	w := NewWriter(tmp)
	for _, record := range recs {
		for _, val := range record.values {
			if err := w.Write([]byte(record.key), []byte(val)); err != nil {
				panic(err)
			}
		}
	}
	if err := w.Close(); err != nil {
		panic(err)
	}
	if _, err = tmp.Seek(0, 0); err != nil {
		panic(err)
	}
	b, err := ioutil.ReadAll(tmp)
	if err != nil {
		panic(err)
	}
	return b
}

func newDB(recs []rec) *Cdb {
	return New(bytes.NewReader(newDBBytes(recs)))
}

func init() {
	b := bytes.NewBuffer(nil)
	for _, rec := range records {
		key := rec.key
		for _, value := range rec.values {
			b.WriteString(fmt.Sprintf("+%d,%d:%s->%s\n", len(key), len(value), key, value))
		}
	}
	b.WriteByte('\n')
	data = b.Bytes()
}
