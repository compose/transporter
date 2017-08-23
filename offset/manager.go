package offset

// Manager defines the necessary functions for providing the ability to manage offsets associated
// with a commitlog.CommitLog.
type Manager interface {
	CommitOffset(Offset, bool) error
	OffsetMap() map[string]uint64
	NewestOffset() int64
}
