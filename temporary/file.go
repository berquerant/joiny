package temporary

import (
	"errors"
	"fmt"
	"os"
)

func NewFile() (*File, error) {
	f, err := os.CreateTemp("", "join")
	if err != nil {
		return nil, err
	}
	return &File{f}, nil
}

type File struct {
	*os.File
}

func (f *File) Close() error {
	if err := f.File.Close(); err != nil {
		return err
	}
	return os.Remove(f.Name())
}

var ErrEmptyFileList = errors.New("EmptyFileList")

func NewFileList(n int) (FileList, error) {
	if n < 1 {
		return nil, ErrEmptyFileList
	}
	r := make([]*File, n)
	for i := range r {
		x, err := NewFile()
		if err != nil {
			for j := 0; j < i-1; j++ {
				r[j].Close()
			}
			return nil, err
		}
		r[i] = x
	}
	return r, nil
}

type FileList []*File

func (f FileList) Close() error {
	var errList []error
	for _, x := range f {
		if err := x.Close(); err != nil {
			errList = append(errList, err)
		}
	}
	if len(errList) > 0 {
		return fmt.Errorf("close error: %v", errList)
	}
	return nil
}
