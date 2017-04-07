package commitlog

import (
	"io"
	"sync"
)

type Reader struct {
	commitlog *CommitLog
	idx       int
	mu        sync.Mutex
	position  int64
}

func (r *Reader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	segments := r.commitlog.Segments()
	segment := segments[r.idx]

	var (
		n, readSize int
		err         error
	)
	for {
		readSize, err = segment.ReadAt(p[n:], int64(r.position))
		n += readSize
		r.position += int64(readSize)
		if readSize != 0 && err == nil {
			continue
		}
		if n == len(p) || err != io.EOF {
			break
		}
		if len(segments) <= r.idx+1 {
			err = io.EOF
			break
		}
		r.idx++
		segment = segments[r.idx]
		r.position = 0
	}

	return n, err
}
