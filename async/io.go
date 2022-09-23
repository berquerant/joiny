package async

import (
	"io"
	"sync"
)

type ReadSeeker interface {
	// Do call f in critical section.
	Do(f func(io.ReadSeeker) error) error
}

type readSeeker struct {
	sync.Mutex
	io.ReadSeeker
}

func (l *readSeeker) Do(f func(io.ReadSeeker) error) error {
	l.Lock()
	defer l.Unlock()
	return f(l)
}

func NewReadSeeker(r io.ReadSeeker) ReadSeeker {
	return &readSeeker{
		ReadSeeker: r,
	}
}
