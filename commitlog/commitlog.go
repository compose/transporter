// a lot of what's here is borrowed from https://github.com/travisjeffery/jocko/

package commitlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/compose/transporter/log"
)

const (
	defaultMaxSegmentBytes = 1024 * 1024 * 100
	logFileSuffix          = ".log"
	cleanedFileSuffix      = ".cleaned"
	swapFileSuffix         = ".swap"
	deletedFileSuffix      = ".deleted"
)

var (
	defaultPath = filepath.Join(os.TempDir(), "transporter")
	encoding    = binary.BigEndian

	// ErrEmptyPath will be returned if the provided path is an empty string
	ErrEmptyPath = errors.New("path is empty")
	// ErrSegmentNotFound is returned with no segment is found given the provided offset
	ErrSegmentNotFound = errors.New("segment not found")
)

// CommitLog is how the rest of the system will interact with the underlying log segments
// to persist and read messages.
type CommitLog struct {
	path            string
	maxSegmentBytes int64

	mu             sync.RWMutex
	segments       []*Segment
	vActiveSegment atomic.Value
}

// OptionFunc is a function that configures a CommitLog.
// It is used in New.
type OptionFunc func(*CommitLog) error

// New creates a new CommitLog for persisting and reading messages.
//
// The caller can configure the CommitLog by passing configuration options
// to the func.
//
// Example:
//
//   c, err := New(
//     WithPath("path/to/dir"),
//     WithMaxSegmentBytes(1024))
//
// An error is also returned when some configuration option is invalid
func New(options ...OptionFunc) (*CommitLog, error) {
	// Set up the client
	c := &CommitLog{
		path:            defaultPath,
		maxSegmentBytes: defaultMaxSegmentBytes,
	}

	// Run the options on it
	for _, option := range options {
		if err := option(c); err != nil {
			return nil, err
		}
	}

	if err := c.init(); err != nil {
		return nil, err
	}

	if err := c.open(); err != nil {
		return nil, err
	}

	return c, nil
}

// WithPath defines the directory where all data will be stored.
func WithPath(path string) OptionFunc {
	return func(c *CommitLog) error {
		if path == "" {
			return ErrEmptyPath
		}
		c.path = path
		return nil
	}
}

// WithMaxSegmentBytes defines the maximum limit a log segment can reach before needing
// to create a new one.
func WithMaxSegmentBytes(max int64) OptionFunc {
	return func(c *CommitLog) error {
		if max > 0 {
			c.maxSegmentBytes = max
		}
		return nil
	}
}

func (c *CommitLog) init() error {
	return os.MkdirAll(c.path, 0755)
}

func (c *CommitLog) open() error {
	files, err := ioutil.ReadDir(c.path)
	if err != nil {
		return err
	}

	// first pass through to clean up any interrupted compactions
	for _, file := range files {
		switch filepath.Ext(file.Name()) {
		case deletedFileSuffix, cleanedFileSuffix:
			os.Remove(filepath.Join(c.path, file.Name()))
		case swapFileSuffix:
			offsetStr := strings.TrimSuffix(file.Name(), swapFileSuffix)
			baseOffset, err := strconv.Atoi(offsetStr)
			if err != nil {
				return err
			}
			os.Rename(
				filepath.Join(c.path, file.Name()),
				filepath.Join(c.path, fmt.Sprintf(LogNameFormat, baseOffset)),
			)
		}
	}

	// now load each segment file
	for _, file := range files {
		if strings.HasSuffix(file.Name(), logFileSuffix) {
			offsetStr := strings.TrimSuffix(file.Name(), logFileSuffix)
			baseOffset, err := strconv.Atoi(offsetStr)
			segment, err := NewSegment(c.path, LogNameFormat, int64(baseOffset), c.maxSegmentBytes)
			if err != nil {
				return err
			}
			// we don't want to keep file handles open unless they are needed
			// if len(c.segments) > 0 {
			// 	c.segments[len(c.segments)-1].Close()
			// }
			c.segments = append(c.segments, segment)
		}
	}
	if len(c.segments) == 0 {
		segment, err := NewSegment(c.path, LogNameFormat, 0, c.maxSegmentBytes)
		if err != nil {
			return err
		}
		c.segments = append(c.segments, segment)
	}
	c.vActiveSegment.Store(c.segments[len(c.segments)-1])
	return nil
}

// Append will convert the set the offset for the provided []byte and then persist
// it to the active segment.
func (c *CommitLog) Append(p []byte) (offset int64, err error) {
	l := Log(p)
	if c.checkSplit() {
		if err := c.split(); err != nil {
			return offset, err
		}
	}
	offset = c.NewestOffset()
	l.PutOffset(offset)
	if _, err := c.activeSegment().Write(l); err != nil {
		return offset, err
	}
	return offset, nil
}

// Close iterates over all segments and calls its Close() func.
func (c *CommitLog) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, segment := range c.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

// NewestOffset obtains the NextOffset of the current segment in use.
func (c *CommitLog) NewestOffset() int64 {
	return c.activeSegment().NextOffset
}

// OldestOffset obtains the BaseOffset from the oldest segment on disk.
func (c *CommitLog) OldestOffset() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.segments[0].BaseOffset
}

// Segments provides access to the underlying segments stored on disk.
func (c *CommitLog) Segments() []*Segment {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.segments
}

// DeleteAll cleans out all data in the path.
func (c *CommitLog) DeleteAll() error {
	if err := c.Close(); err != nil {
		return err
	}
	return os.RemoveAll(c.path)
}

// NewReader returns an io.Reader based on the provider offset.
func (c *CommitLog) NewReader(offset int64) (io.Reader, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	log.With("num_segments", len(c.segments)).
		With("offset", offset).
		Infoln("searching segments")

	// in the event there has been data committed to the segment but no offset for a path,
	// then we need to create a reader that starts from the very beginning
	if offset < 0 && len(c.segments) > 0 {
		// if err := c.segments[0].Open(); err != nil {
		// 	log.Errorf("unable to open segment, %s", err)
		// }
		return &Reader{commitlog: c, idx: 0, position: 0}, nil
	}

	var idx int
	for i := 0; i < len(c.segments); i++ {
		idx = i
		if (i + 1) != len(c.segments) {
			lowerOffset := c.segments[i].BaseOffset
			upperOffset := c.segments[i+1].BaseOffset
			log.With("lower_offset", lowerOffset).
				With("upper_offset", upperOffset).
				With("offset", offset).
				With("segment", idx).
				Debugln("checking if offset in segment")
			if offset >= lowerOffset && offset < upperOffset {
				break
			}
		}
	}

	log.With("offset", offset).With("segment_index", idx).Debugln("finding offset in segment")
	// if err := c.segments[idx].Open(); err != nil {
	// 	log.Errorf("unable to open segment, %s", err)
	// }
	position, err := c.segments[idx].FindOffsetPosition(uint64(offset))
	if err != nil {
		return nil, err
	}

	return &Reader{
		commitlog: c,
		idx:       idx,
		position:  position,
	}, nil
}

func (c *CommitLog) replaceSegment(newSegment, oldSegment *Segment) error {
	log.With("new_segment", newSegment.path).
		With("old_segment", oldSegment.path).
		Infoln("replacing segment...")
	c.mu.Lock()
	defer c.mu.Unlock()
	log.With("new_segment", newSegment.path).
		With("format", swapNameFormat).
		Infoln("renaming")
	newSegment.rename(c.path, swapNameFormat)
	// TODO: this may become ineffienct but works for now
	for i, s := range c.segments {
		if s == oldSegment {
			c.segments[i] = newSegment
			break
		}
	}
	// TODO: make this async
	log.With("old_segment", c.path).Infoln("configuring for deletion")
	oldSegment.rename(c.path, deleteNameFormat)
	log.With("old_segment", oldSegment.log.Name()).Infoln("deleting segment")
	os.Remove(oldSegment.log.Name())

	log.With("new_segment", newSegment.path).
		With("format", LogNameFormat).
		Infoln("renaming")
	newSegment.rename(c.path, LogNameFormat)

	// no need to keep a file handle open once compaction has completed
	// if err := newSegment.Close(); err != nil {
	// 	log.With("new_segment", newSegment.path).Errorf("failed to Close, %s", err)
	// }
	log.With("new_segment", newSegment.path).
		With("old_segment", oldSegment.path).
		Infoln("segment replacement complete")
	return nil
}

func (c *CommitLog) activeSegment() *Segment {
	return c.vActiveSegment.Load().(*Segment)
}

func (c *CommitLog) checkSplit() bool {
	return c.activeSegment().IsFull()
}

func (c *CommitLog) split() error {
	segment, err := NewSegment(c.path, LogNameFormat, c.NewestOffset(), c.maxSegmentBytes)
	log.With("segment", segment.path).Infoln("new segment created")
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.segments = append(c.segments, segment)
	// c.activeSegment().Close()
	c.vActiveSegment.Store(segment)
	c.mu.Unlock()
	return nil
}
