package commitlog

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	logNameFormat = "%020d.log"
)

// Segment handles reading and writing to the underlying files on disk.
type Segment struct {
	writer   io.Writer
	reader   io.Reader
	log      *os.File
	maxBytes int64

	BaseOffset int64
	NextOffset int64
	Position   int64

	sync.Mutex
}

// NewSegment creates a new instance of Segment with the provided parameters
// and initializes its NextOffset and Position should the file be non-empty.
func NewSegment(path string, baseOffset int64, maxBytes int64) (*Segment, error) {
	logPath := filepath.Join(path, fmt.Sprintf(logNameFormat, baseOffset))
	log, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	s := &Segment{
		log:        log,
		writer:     log,
		reader:     log,
		maxBytes:   maxBytes,
		BaseOffset: baseOffset,
		NextOffset: baseOffset,
	}

	err = s.init()
	if err == io.EOF {
		return s, nil
	}

	return s, err
}

func (s *Segment) init() error {
	if _, err := s.log.Seek(0, 0); err != nil {
		return err
	}

	for {
		b := new(bytes.Buffer)
		// get offset and size
		_, err := io.CopyN(b, s.log, 8)
		if err != nil {
			return err
		}
		s.NextOffset = int64(encoding.Uint64(b.Bytes()[offsetPos:8]))

		_, err = io.CopyN(b, s.log, 4)
		if err != nil {
			return err
		}
		size := int64(encoding.Uint32(b.Bytes()[sizePos:12]))

		s.Position += size + logEntryHeaderLen
		s.NextOffset++

		// add 9 to size to include the timestamp and attribute
		_, err = s.log.Seek(size+9, 1)
		if err != nil {
			return err
		}
	}
}

// IsFull determines whether the current size of the segment is greater than or equal to the
// maxBytes configured.
func (s *Segment) IsFull() bool {
	s.Lock()
	defer s.Unlock()
	return s.Position >= s.maxBytes
}

func (s *Segment) Write(p []byte) (n int, err error) {
	s.Lock()
	defer s.Unlock()
	n, err = s.writer.Write(p)
	if err != nil {
		return n, err
	}
	s.NextOffset++
	s.Position += int64(n)
	return n, nil
}

// Close closes the read/write access to the underlying file.
func (s *Segment) Close() error {
	s.Lock()
	defer s.Unlock()
	return s.log.Close()
}
