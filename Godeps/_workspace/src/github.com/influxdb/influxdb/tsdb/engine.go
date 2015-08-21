package tsdb

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/boltdb/bolt"
)

var (
	// ErrFormatNotFound is returned when no format can be determined from a path.
	ErrFormatNotFound = errors.New("format not found")
)

// DefaultEngine is the default engine used by the shard when initializing.
const DefaultEngine = "v1"

// Engine represents a swappable storage engine for the shard.
type Engine interface {
	Open() error
	Close() error

	SetLogOutput(io.Writer)
	LoadMetadataIndex(index *DatabaseIndex, measurementFields map[string]*MeasurementFields) error

	Begin(writable bool) (Tx, error)
	WritePoints(points []Point, measurementFieldsToSave map[string]*MeasurementFields, seriesToCreate []*SeriesCreate) error
	DeleteSeries(keys []string) error
	DeleteMeasurement(name string, seriesKeys []string) error
	SeriesCount() (n int, err error)
}

// NewEngineFunc creates a new engine.
type NewEngineFunc func(path string, options EngineOptions) Engine

// newEngineFuncs is a lookup of engine constructors by name.
var newEngineFuncs = make(map[string]NewEngineFunc)

// RegisterEngine registers a storage engine initializer by name.
func RegisterEngine(name string, fn NewEngineFunc) {
	if _, ok := newEngineFuncs[name]; ok {
		panic("engine already registered: " + name)
	}
	newEngineFuncs[name] = fn
}

// NewEngine returns an instance of an engine based on its format.
// If the path does not exist then the DefaultFormat is used.
func NewEngine(path string, options EngineOptions) (Engine, error) {
	// Create a new engine
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return newEngineFuncs[DefaultEngine](path, options), nil
	}

	// Only bolt-based backends are currently supported so open it and check the format.
	var format string
	if err := func() error {
		db, err := bolt.Open(path, 0666, &bolt.Options{Timeout: 1 * time.Second})
		if err != nil {
			return err
		}
		defer db.Close()

		return db.View(func(tx *bolt.Tx) error {
			// Retrieve the meta bucket.
			b := tx.Bucket([]byte("meta"))

			// If no format is specified then it must be an original v1 database.
			if b == nil {
				format = "v1"
				return nil
			}

			// Save the format.
			format = string(b.Get([]byte("format")))
			return nil
		})
	}(); err != nil {
		return nil, err
	}

	// Lookup engine by format.
	fn := newEngineFuncs[format]
	if fn == nil {
		return nil, fmt.Errorf("invalid engine format: %q", format)
	}

	return fn(path, options), nil
}

// EngineOptions represents the options used to initialize the engine.
type EngineOptions struct {
	MaxWALSize             int
	WALFlushInterval       time.Duration
	WALPartitionFlushDelay time.Duration
}

// NewEngineOptions returns the default options.
func NewEngineOptions() EngineOptions {
	return EngineOptions{
		MaxWALSize:             DefaultMaxWALSize,
		WALFlushInterval:       DefaultWALFlushInterval,
		WALPartitionFlushDelay: DefaultWALPartitionFlushDelay,
	}
}

// Tx represents a transaction.
type Tx interface {
	io.WriterTo

	Cursor(series string) Cursor
	Size() int64
	Commit() error
	Rollback() error
}

// Cursor represents an iterator over a series.
type Cursor interface {
	Seek(seek []byte) (key, value []byte)
	Next() (key, value []byte)
}
