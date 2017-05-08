package commitlog

import (
	"io"
	"os"
	"sync"
	"time"

	"github.com/compose/transporter/log"
)

// Compactor defines the necessary functions for performing compaction of
// log segments.
type Compactor interface {
	Compact(uint64, []*Segment)
}

var (
	_ Compactor = &NamespaceCompactor{}
)

// NamespaceCompactor compact individual segments based on the key which
// is the source adaptor namespace.
type NamespaceCompactor struct {
	log *CommitLog
}

func (c *NamespaceCompactor) Compact(offset uint64, segments []*Segment) {
	log.With("num_segments", len(segments)).Infoln("starting compaction...")
	var wg sync.WaitGroup
	wg.Add(len(segments))
	for _, segment := range segments {
		log.With("segment", segment.log.Name()).Infoln("compacting...")
		go c.compactSegment(offset, &wg, segment)
	}
	wg.Wait()
}

func (c *NamespaceCompactor) compactSegment(offset uint64, wg *sync.WaitGroup, segment *Segment) {
	defer wg.Done()
	if err := segment.Open(); err != nil {
		log.With("segment", segment.path).Errorf("unable to open segment, %s", err)
	}
	r := &segmentReader{s: segment, position: 0}
	entryMap := make(map[string]LogEntry)
	for {
		o, e, err := ReadEntry(r)
		if err == io.EOF {
			break
		} else if err != nil {
			log.Errorf("failed to compact segment, %s", err)
			return
		}
		if o >= offset {
			log.Infof("unable to compact segment (%s), contains unread offset, %d", segment.log.Name(), offset)
			return
		}
		entryMap[string(e.Key)] = e
	}
	newSegment, err := NewSegment(c.log.path,
		cleanNameFormat,
		segment.BaseOffset,
		c.log.maxSegmentBytes)
	if err != nil {
		log.Errorf("failed to create cleaned segment, %s", err)
		return
	}
	for _, le := range entryMap {
		l := NewLogFromEntry(le)
		if _, err := newSegment.Write(l); err != nil {
			log.Errorf("failed writing to cleaned segment, %s", err)
			return
		}
	}
	stat, err := segment.log.Stat()
	if err != nil {
		log.Infof("unable to get stats for segment, %s", err)
	}
	os.Chtimes(newSegment.log.Name(), stat.ModTime(), time.Now())
	if err := c.log.replaceSegment(newSegment, segment); err != nil {
		log.Errorf("failed to replace segment, %s", err)
	}
	log.With("segment", segment.log.Name()).Infoln("compaction complete")
}

type segmentReader struct {
	s        *Segment
	position int64
}

func (r *segmentReader) Read(p []byte) (int, error) {
	n, err := r.s.ReadAt(p, r.position)
	if err != nil {
		return n, err
	}
	r.position += int64(n)
	return n, nil
}
