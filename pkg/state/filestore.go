package state

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/compose/transporter/pkg/message"
)

// listen for signals, and send stops to the generator
var (
	chQuit = make(chan os.Signal)
)

type filestore struct {
	key         string
	filename    string
	flushTicker *time.Ticker
	states      map[string]*MsgState
}

func NewFilestore(key, filename string, interval time.Duration) SessionStore {
	filestore := &filestore{
		key:         key,
		filename:    filename,
		flushTicker: time.NewTicker(interval),
		states:      make(map[string]*MsgState),
	}
	go filestore.startFlusher()
	signal.Notify(chQuit, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-chQuit:
			fmt.Println("Got signal, wrapping up")
			filestore.flushTicker.Stop()
			filestore.flushToDisk()
		}
	}()
	return filestore
}

func (f *filestore) startFlusher() {
	for _ = range f.flushTicker.C {
		f.flushToDisk()
	}
}

func (f *filestore) flushToDisk() error {
	b := new(bytes.Buffer)
	enc := gob.NewEncoder(b)
	err := enc.Encode(f.states)
	if err != nil {
		return err
	}

	fh, eopen := os.OpenFile(f.filename, os.O_CREATE|os.O_WRONLY, 0666)
	defer fh.Close()
	if eopen != nil {
		return eopen
	}
	n, e := fh.Write(b.Bytes())
	if e != nil {
		return e
	}
	fmt.Fprintf(os.Stderr, "%d bytes successfully written to file\n", n)
	return nil
}

func (f *filestore) Set(path string, msg *message.Msg) error {
	var id = ""
	if msg.Id != nil {
		id = msg.IdAsString()
	}
	f.states[f.key+"-"+path] = &MsgState{Id: id, Timestamp: msg.Timestamp}
	return nil
}

func (f *filestore) Get(path string) (string, int64, error) {
	currentState := f.states[f.key+"-"+path]

	if currentState == nil {
		fh, err := os.Open(f.filename)
		if err != nil {
			return "", 0, err
		}
		states := make(map[string]*MsgState)
		dec := gob.NewDecoder(fh)
		err = dec.Decode(&states)
		if err != nil {
			return "", 0, err
		}
		currentState = states[f.key+"-"+path]
	}
	return currentState.Id, currentState.Timestamp, nil
}
