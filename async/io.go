package async

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/berquerant/cache"
)

type ReadSeeker interface {
	// Do call f in critical section.
	Do(f func(io.ReadSeeker) error) error
}

type readSeeker struct {
	sync.Mutex
	io.ReadSeeker
}

func (r *readSeeker) Do(f func(io.ReadSeeker) error) error {
	r.Lock()
	defer r.Unlock()
	return f(r)
}

func NewReadSeeker(r io.ReadSeeker) ReadSeeker {
	return &readSeeker{
		ReadSeeker: r,
	}
}

type CachedReader interface {
	Read(offset int64, size int) ([]byte, error)
}

func NewCachedReader(size int, r ReadSeeker) (CachedReader, error) {
	c := &cachedReader{
		r:    r,
		size: size,
	}
	if err := c.init(); err != nil {
		return nil, err
	}
	return c, nil
}

type cachedReaderKey struct {
	offset int64
	size   int
}

type cachedReader struct {
	db   cache.Cache[cachedReaderKey, []byte]
	r    ReadSeeker
	size int
}

func (c *cachedReader) init() error {
	db, err := cache.NewLRU(c.size, c.source)
	if err != nil {
		return fmt.Errorf("CacheReader: failed to build cache %w", err)
	}
	c.db = db
	return nil
}

func (c *cachedReader) source(key cachedReaderKey) ([]byte, error) {
	var result []byte
	if err := c.r.Do(func(data io.ReadSeeker) error {
		if _, err := data.Seek(key.offset, os.SEEK_SET); err != nil {
			return err
		}

		result = make([]byte, key.size)
		if _, err := data.Read(result); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *cachedReader) Read(offset int64, size int) ([]byte, error) {
	return c.db.Get(cachedReaderKey{
		offset: offset,
		size:   size,
	})
}
