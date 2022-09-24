package joiner

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/berquerant/joiny/async"
	"github.com/berquerant/logger"
)

// KeyFunc extracts a key from a line.
type KeyFunc func(string) (string, error)

//go:generate go run github.com/berquerant/dataclass@v0.1.0 -type Item -field "Key string,Offset int64,Size int" -output index_dataclass_item_generated.go

//go:generate go run github.com/berquerant/dataclass@v0.1.0 -type ScannedItem -field "Line string,Item Item" -output index_dataclass_scanneditem_generated.go

type itemListMap map[string][]Item

func (m itemListMap) get(key string) ([]Item, bool) {
	got, ok := m[key]
	return got, ok
}

func (m itemListMap) add(key string, item Item) {
	m[key] = append(m[key], item)
}

// Index is an in-memory word-to-lines index.
// This is read-only, underlying data source (file) must be also read-only.
type Index interface {
	KeyFunc() KeyFunc
	Get(key string) ([]Item, bool)
	Read(item Item) (ScannedItem, error)
	Scan(ctx context.Context) <-chan ScannedItem
	AllItems(ctx context.Context) <-chan Item
}

func NewIndex(ctx context.Context, data async.ReadSeeker, key KeyFunc) (Index, error) {
	s := &index{
		data: data,
		key:  key,
		val:  make(map[string][]Item),
	}
	if err := s.init(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

type index struct {
	data async.ReadSeeker
	key  KeyFunc
	val  itemListMap
}

func (idx *index) init(ctx context.Context) error {
	return idx.data.Do(func(data io.ReadSeeker) error {
		if _, err := data.Seek(0, os.SEEK_SET); err != nil {
			return fmt.Errorf("Index init: %w", err)
		}

		var (
			offset    int64
			isEOF     bool
			lineCount int
			itemCount int
			startAt   = time.Now()
			r         = bufio.NewReader(data)
		)
		for !isEOF {
			if async.Done(ctx) {
				return fmt.Errorf("Index init: %w", ctx.Err())
			}
			line, err := r.ReadBytes('\n')
			isEOF = errors.Is(err, io.EOF)
			if err != nil && !isEOF {
				return fmt.Errorf("Index init: offset %d %w", offset, err)
			}

			lineCount++
			size := len(line)
			lineStr := strings.TrimRight(string(line), "\n")
			if lineStr == "" {
				offset += int64(size)
				continue
			}

			key, err := idx.key(lineStr) // generate index key
			if err != nil {
				return fmt.Errorf("Index init: %s offset %d %w", lineStr, offset, err)
			}
			logger.G().Debug("New Item: key %s line %s offset %d size %d", key, lineStr, offset, size)
			// record the word and it's offset of the head of the line
			idx.val.add(key, NewItem(key, offset, size))
			offset += int64(size)
			itemCount++
		}
		logger.G().Debug("Index init: done, read %d bytes %d lines %d keys %d items %s elapsed",
			offset, lineCount, len(idx.val), itemCount, time.Since(startAt),
		)
		return nil
	})
}

func (idx *index) KeyFunc() KeyFunc { return idx.key }

func (idx *index) Get(key string) ([]Item, bool) {
	// no lock because index is readonly
	return idx.val.get(key)
}

func (idx *index) Read(item Item) (ScannedItem, error) {
	var result ScannedItem
	if err := idx.data.Do(func(data io.ReadSeeker) error {
		if _, err := data.Seek(item.Offset(), os.SEEK_SET); err != nil {
			return fmt.Errorf("Read index: key %s offset %d %w", item.Key(), item.Offset(), err)
		}
		b := make([]byte, item.Size())
		if _, err := data.Read(b); err != nil {
			return fmt.Errorf("Read index: key %s offset %d size %d %w", item.Key(), item.Offset(), item.Size(), err)
		}
		r := strings.TrimRight(string(b), "\n")
		logger.G().Trace("Read Index: %v return %v", item, r)
		result = NewScannedItem(r, item)
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

func (idx *index) Scan(ctx context.Context) <-chan ScannedItem {
	resultC := make(chan ScannedItem, 100)
	go func() {
		defer close(resultC)
		for _, itemList := range idx.val {
			for _, item := range itemList {
				if async.Done(ctx) {
					return
				}
				r, err := idx.Read(item)
				if err != nil {
					logger.G().Error("Scan: failed to read %v, %v", item, err)
					return
				}
				resultC <- r
			}
		}
	}()
	return resultC
}

func (idx *index) AllItems(ctx context.Context) <-chan Item {
	resultC := make(chan Item, 100)
	go func() {
		defer close(resultC)
		for _, itemList := range idx.val {
			for _, item := range itemList {
				if async.Done(ctx) {
					return
				}
				resultC <- item
			}
		}
	}()
	return resultC
}
