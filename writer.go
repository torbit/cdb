package cdb

import (
	"fmt"
	"io"
)

// Writer provides a simple interface for creating CDBs by wrapping the Make
// function.
//
// Not threadsafe.
type Writer struct {
	pipeWriter *io.PipeWriter
	doneCh     chan error
	makeErr    error
}

func NewWriter(ws io.WriteSeeker) *Writer {
	pipeReader, pipeWriter := io.Pipe()
	w := &Writer{
		pipeWriter: pipeWriter,
		doneCh:     make(chan error, 1),
	}
	go func() {
		defer pipeReader.Close()
		w.doneCh <- Make(ws, pipeReader)
	}()
	return w
}

func (w *Writer) Write(key, val []byte) error {
	select {
	case err := <-w.doneCh:
		w.makeErr = err
	default:
	}
	if w.makeErr != nil {
		return w.makeErr
	}
	_, err := fmt.Fprintf(w.pipeWriter, "+%v,%v:%s->%s\n", len(key), len(val), key, val)
	return err
}

func (w *Writer) Close() error {
	if w.makeErr != nil {
		return w.makeErr
	}
	w.pipeWriter.Write([]byte("\n"))
	w.pipeWriter.Close()
	return <-w.doneCh
}
