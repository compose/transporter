package commitlog

// Log is a alias type for []byte.
type Log []byte

// PutOffset sets the provided offset for the given Log.
func (l Log) PutOffset(offset int64) {
	encoding.PutUint64(l[offsetPos:sizePos], uint64(offset))
}
