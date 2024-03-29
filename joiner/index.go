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
	"github.com/berquerant/joiny/logx"
)

// KeyFunc extracts a key from a line.
type KeyFunc func(string) (string, error)

//go:generate go run github.com/berquerant/dataclass@v0.3.1 -type Item -field "Key string|Offset int64|Size int" -output index_dataclass_item_generated.go

//go:generate go run github.com/berquerant/dataclass@v0.3.1 -type ScannedItem -field "Line string|Item Item" -output index_dataclass_scanneditem_generated.go

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

type index struct {
	data async.CachedReader
	key  KeyFunc
	val  itemListMap
}

func newIndex(data async.CachedReader, key KeyFunc, val itemListMap) Index {
	return &index{
		data: data,
		key:  key,
		val:  val,
	}
}

type IndexLoader interface {
	Load(ctx context.Context, key ...KeyFunc) ([]Index, error)
}

func NewIndexLoader(data async.ReadSeeker, indexCacheSize int) IndexLoader {
	return &indexLoader{
		data:           data,
		indexCacheSize: indexCacheSize,
	}
}

type indexLoader struct {
	data           async.ReadSeeker
	indexCacheSize int
}

func (ldr *indexLoader) Load(ctx context.Context, key ...KeyFunc) ([]Index, error) {
	logx.G().Debug("IndexLoader: begin", logx.I("index", len(key)))

	vals := make([]itemListMap, len(key))
	for i := range vals {
		vals[i] = make(map[string][]Item)
	}

	if err := ldr.data.Do(func(data io.ReadSeeker) error {
		if _, err := data.Seek(0, os.SEEK_SET); err != nil {
			return fmt.Errorf("init: %w", err)
		}

		var (
			offset    int64
			isEOF     bool
			lineCount int
			itemCount = make([]int, len(key))
			keySize   = make([]int, len(key))
			startAt   = time.Now()
			r         = bufio.NewReader(data)
		)
		for !isEOF {
			if async.Done(ctx) {
				return fmt.Errorf("load: %w", ctx.Err())
			}
			line, err := r.ReadBytes('\n')
			isEOF = errors.Is(err, io.EOF)
			if err != nil && !isEOF {
				return fmt.Errorf("read: offset %d %w", offset, err)
			}

			lineCount++
			size := len(line)
			lineStr := strings.TrimRight(string(line), "\n")
			if lineStr == "" {
				offset += int64(size)
				continue
			}

			for i, kf := range key {
				k, err := kf(lineStr)
				if err != nil {
					return fmt.Errorf("key[%d]: %s offset %d %w", i, lineStr, offset, err)
				}
				kSize := len(k)
				logx.G().Debug("IndexLoader: new item",
					logx.I("item", i),
					logx.S("line", lineStr),
					logx.I("size", size),
					logx.I("offset", offset),
					logx.I("keysize", kSize),
					logx.S("key", k),
				)
				vals[i].add(k, NewItem(k, offset, size))
				itemCount[i]++
				keySize[i] += kSize
			}
			offset += int64(size)
		}

		for i, ic := range itemCount {
			logx.G().Debug("IndexLoader: done", logx.I("key_index", i), logx.I("size", len(key)), logx.I("item", ic))
		}
		logx.G().Debug("IndexLoader: done",
			logx.I("bytes", offset),
			logx.I("key", len(vals[0])),
			logx.I("line", lineCount),
			logx.D("elapsed", time.Since(startAt)),
		)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("IndexLoader: %w", err)
	}

	indexList := make([]Index, len(vals))
	for i, val := range vals {
		c, err := async.NewCachedReader(ldr.indexCacheSize, ldr.data)
		if err != nil {
			return nil, fmt.Errorf("IndexLoader: %w", err)
		}
		indexList[i] = &index{
			data: c,
			key:  key[i],
			val:  val,
		}
	}
	return indexList, nil
}

func (idx *index) KeyFunc() KeyFunc { return idx.key }

func (idx *index) Get(key string) ([]Item, bool) {
	// no lock because index is readonly
	return idx.val.get(key)
}

func (idx *index) Read(item Item) (ScannedItem, error) {
	b, err := idx.data.Read(item.Offset(), item.Size())
	if err != nil {
		return nil, fmt.Errorf("Read Index: key %s offset %d size %d %w", item.Key(), item.Offset(), item.Size(), err)
	}

	r := strings.TrimRight(string(b), "\n")
	logx.G().Trace("Read Index", logx.Any("item", item), logx.Any("return", r))
	return NewScannedItem(r, item), nil
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
					logx.G().Error("Scan: failed to read", logx.Any("item", item), logx.Err(err))
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
