package commitlog

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/compose/transporter/log"
)

const (
	// LogNameFormat defines the filename structure for active segments.
	LogNameFormat = "%020d.log"

	cleanNameFormat = "%020d.cleaned"

	deleteNameFormat = "%020d.deleted"

	swapNameFormat = "%020d.swap"
)

var (
	// ErrOffsetNotFound is returned when the requested offset is not in the segment.
	ErrOffsetNotFound = errors.New("offset not found")
)

// Segment handles reading and writing to the underlying files on disk.
type Segment struct {
	writer   io.Writer
	reader   io.Reader
	log      *os.File
	path     string
	maxBytes int64

	BaseOffset int64
	NextOffset int64
	Position   int64

	sync.Mutex
}

// NewSegment creates a new instance of Segment with the provided parameters
// and initializes its NextOffset and Position should the file be non-empty.
func NewSegment(path, format string, baseOffset int64, maxBytes int64) (*Segment, error) {
	logPath := filepath.Join(path, fmt.Sprintf(format, baseOffset))
	log, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	s := &Segment{
		log:        log,
		path:       logPath,
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

func (s *Segment) rename(path, newFormat string) {
	s.Lock()
	defer s.Unlock()
	logPath := filepath.Join(path, fmt.Sprintf(newFormat, s.BaseOffset))
	currentName := s.log.Name()
	s.log.Close()
	if err := os.Rename(currentName, logPath); err != nil {
		log.Errorln(err)
	}
	newLog, err := os.OpenFile(logPath, os.O_RDWR, 0666)
	if err != nil {
		log.Errorln(err)
	}
	s.log = newLog
	s.reader = newLog
	s.writer = newLog
	s.path = logPath
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

func (s *Segment) ReadAt(p []byte, off int64) (n int, err error) {
	s.Lock()
	defer s.Unlock()
	return s.log.ReadAt(p, off)
}

// func (s *Segment) Open() error {
// 	s.Lock()
// 	defer s.Unlock()
// 	l, err := os.OpenFile(s.path, os.O_RDWR, 0666)
// 	if err != nil {
// 		return err
// 	}
// 	s.log = l
// 	s.reader = l
// 	s.writer = l
// 	return nil
// }

// Close closes the read/write access to the underlying file.
func (s *Segment) Close() error {
	s.Lock()
	defer s.Unlock()
	return s.log.Close()
}

func (s *Segment) FindOffsetPosition(offset uint64) (int64, error) {
	if _, err := s.log.Seek(0, 0); err != nil {
		return 0, err
	}

	var position int64
	for {
		b := new(bytes.Buffer)
		// get offset and size
		_, err := io.CopyN(b, s.log, 8)
		if err != nil {
			return position, ErrOffsetNotFound
		}
		o := encoding.Uint64(b.Bytes()[offsetPos:8])

		_, err = io.CopyN(b, s.log, 4)
		if err != nil {
			return position, ErrOffsetNotFound
		}
		size := int64(encoding.Uint32(b.Bytes()[sizePos:12]))

		if offset == o {
			log.With("position", position).With("offset", o).Infoln("found offset position")
			return position, nil
		}
		position += size + logEntryHeaderLen

		// add 9 to size to include the timestamp and attribute
		_, err = s.log.Seek(size+9, 1)
		if err != nil {
			return 0, err
		}
	}
}
