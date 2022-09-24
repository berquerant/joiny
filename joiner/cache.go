package joiner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/berquerant/joiny/async"
	"github.com/berquerant/joiny/cc/joinkey"
	"github.com/berquerant/joiny/slicing"
	"github.com/berquerant/logger"
	"golang.org/x/sync/errgroup"
)

type Cache interface {
	Get(src, col int) (Index, bool)
	GetBySrc(src int) ([]Index, bool)
	Delimiter() string
}

type cacheKey struct {
	src int
	col int
}

type cache struct {
	val       map[cacheKey]Index
	srcIdx    map[int][]Index
	delimiter string
}

func (c *cache) Delimiter() string { return c.delimiter }

func (c *cache) Get(src, col int) (Index, bool) {
	idx, found := c.val[cacheKey{
		src: src,
		col: col,
	}]
	return idx, found
}

func (c *cache) GetBySrc(src int) ([]Index, bool) {
	idxs, found := c.srcIdx[src]
	return idxs, found
}

type Location interface {
	Source() int
	Column() int
}

type CacheBuilder interface {
	Build(ctx context.Context) (Cache, error)
}

func RelationListToLocationList(relationList []*joinkey.Relation) []Location {
	var (
		i int
		r = make([]Location, len(relationList)*2)
	)
	for _, rel := range relationList {
		r[i] = rel.Left.Add(-1, -1) // zero-based
		i++
		r[i] = rel.Right.Add(-1, -1)
		i++
	}
	return r
}

func NewCacheBuilder(dataList []io.ReadSeeker, locationList []Location, delimiter string) CacheBuilder {
	lockedDataList := make([]async.ReadSeeker, len(dataList))
	for i, d := range dataList {
		lockedDataList[i] = async.NewReadSeeker(d)
	}
	return &cacheBuilder{
		dataList:     lockedDataList,
		delimiter:    delimiter,
		locationList: locationList,
	}
}

type cacheBuilder struct {
	dataList     []async.ReadSeeker
	delimiter    string
	locationList []Location
}

var ErrInvalidKey = errors.New("InvalidKey")

const cacheBuilderThread = 4

func (c *cacheBuilder) Build(ctx context.Context) (Cache, error) {
	logger.G().Debug("BuilderBuildCache: start, %d sources %d locations", len(c.dataList), len(c.locationList))
	startAt := time.Now()

	cacheKeyList := make([]cacheKey, len(c.locationList))
	for i, loc := range c.locationList {
		cacheKeyList[i] = cacheKey{
			src: loc.Source(),
			col: loc.Column(),
		}
	}
	cacheKeyList = slicing.Uniq(cacheKeyList, func(v cacheKey) cacheKey { return v })
	defer func() {
		logger.G().Debug("BuilderBuildCache: end, caches %d elapsed %s", len(cacheKeyList), time.Since(startAt))
	}()

	type resItem struct {
		key cacheKey
		idx Index
	}

	var (
		resC = make(chan *resItem, len(cacheKeyList))
		eg   errgroup.Group
	)
	eg.SetLimit(cacheBuilderThread)

	for _, ck := range cacheKeyList {
		ck := ck
		key := c.keyFunc(ck.col)
		if !slicing.InRange(c.dataList, ck.src) {
			return nil, fmt.Errorf("Build Cache: %w failed to get index %d (source %d), source len %d ck %v",
				ErrInvalidKey, ck.src, ck.src+1, len(c.dataList), ck,
			)
		}
		data := c.dataList[ck.src]
		eg.Go(func() error {
			logger.G().Debug("Build Cache: begin %v", ck)
			// need lock because seek the file
			idx, err := NewIndex(ctx, data, key)
			logger.G().Debug("Build Cache: end %v", ck)
			if err != nil {
				return fmt.Errorf("Build Cache: %w loc %v", err, ck)
			}
			resC <- &resItem{
				key: ck,
				idx: idx,
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, fmt.Errorf("Build Cache: %w", err)
	}
	close(resC)

	var (
		srcIdx = make(map[int][]Index)
		val    = make(map[cacheKey]Index, len(resC))
	)
	for x := range resC {
		srcIdx[x.key.src] = append(srcIdx[x.key.src], x.idx)
		val[x.key] = x.idx
	}
	return &cache{
		val:       val,
		srcIdx:    srcIdx,
		delimiter: c.delimiter,
	}, nil
}

var ErrNewKeyFailure = errors.New("NewKeyFailure")

func (c *cacheBuilder) keyFunc(col int) KeyFunc {
	return func(v string) (string, error) {
		ss := strings.Split(v, c.delimiter)
		if col >= 0 && col < len(ss) {
			return ss[col], nil
		}
		return "", fmt.Errorf("Build cache: %w col %d delim %s line %s", ErrNewKeyFailure, col, c.delimiter, v)
	}
}
