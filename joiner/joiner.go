package joiner

import (
	"context"
	"fmt"
	"sort"

	"github.com/berquerant/joiny/async"
	"github.com/berquerant/joiny/cc/joinkey"
	"github.com/berquerant/joiny/logx"
	"golang.org/x/exp/slices"
)

func (s *selectItem) String() string { return fmt.Sprintf("SelectItem(%d, %+v)", s.source, s.item) }

type SelectItemList map[int]SelectItem

func (s SelectItemList) Set(item SelectItem) {
	s[item.Source()] = item
}

func (s SelectItemList) Clone() SelectItemList {
	r := make(map[int]SelectItem, len(s))
	for k, v := range s {
		r[k] = v
	}
	return r
}

func (s SelectItemList) Keys() []int {
	var (
		i    int
		keys = make([]int, len(s))
	)
	for k := range s {
		keys[i] = k
		i++
	}
	sort.Ints(keys)
	return keys
}

func (s SelectItemList) Sorted() []SelectItem {
	keys := s.Keys()
	r := make([]SelectItem, len(keys))
	for i, k := range keys {
		r[i] = s[k]
	}
	return r
}

type RelationJoiner interface {
	// FullJoin links records with cross join.
	FullJoin(ctx context.Context, rel *joinkey.Relation) <-chan SelectItemList
	// Join links given rows and the other records.
	// Fallback to FullJoin if rowC is nil.
	Join(ctx context.Context, rel *joinkey.Relation, rowC <-chan SelectItemList) <-chan SelectItemList
}

func NewRelationJoiner(cache Cache) RelationJoiner {
	return &relationJoiner{
		cache: cache,
	}
}

type relationJoiner struct {
	cache Cache
}

func (r *relationJoiner) FullJoin(ctx context.Context, rel *joinkey.Relation) <-chan SelectItemList {
	resultC := make(chan SelectItemList, 100)
	go func() {
		defer close(resultC)
		lKey, rKey := rel.Left.Add(-1, -1), rel.Right.Add(-1, -1)
		lIndex, ok := r.cache.Get(lKey.Src, lKey.Col)
		if !ok {
			logx.G().Warn("FullJoin: left index not found", logx.Any("key", lKey))
			return
		}
		rIndex, ok := r.cache.Get(rKey.Src, rKey.Col)
		if !ok {
			logx.G().Warn("FullJoin: right index not found", logx.Any("key", rKey))
			return
		}

		// cross join for all items
		for lItem := range lIndex.AllItems(ctx) {
			rItemList, ok := rIndex.Get(lItem.Key())
			if !ok {
				continue
			}
			list := make(SelectItemList)
			list.Set(NewSelectItem(lKey.Src, lItem))
			for _, rItem := range rItemList {
				if async.Done(ctx) {
					return
				}
				l := list.Clone()
				l.Set(NewSelectItem(rKey.Src, rItem))
				logx.G().Debug("FullJoin", logx.Any("left", lKey), logx.Any("right", rKey), logx.Any("list", l))
				resultC <- l
			}
		}
	}()
	return resultC
}

func (r *relationJoiner) Join(ctx context.Context, rel *joinkey.Relation, rowC <-chan SelectItemList) <-chan SelectItemList {
	if rowC == nil {
		return r.FullJoin(ctx, rel)
	}

	resultC := make(chan SelectItemList, 100)
	go func() {
		defer close(resultC)

		lKey, rKey := rel.Left.Add(-1, -1), rel.Right.Add(-1, -1) // into zero-based
		lIndex, ok := r.cache.Get(lKey.Src, lKey.Col)
		if !ok {
			logx.G().Warn("Join: left index not found", logx.Any("key", lKey))
			return
		}
		rIndex, ok := r.cache.Get(rKey.Src, rKey.Col)
		if !ok {
			logx.G().Warn("Join: right index not found", logx.Any("key", rKey))
			return
		}

		var (
			isTop   = true
			sources []int
		)
		for row := range rowC {
			if async.Done(ctx) {
				return
			}
			baseInfo := fmt.Sprintf("lkey %v rkey %v row %v", lKey, rKey, row)
			logx.G().Debug("Join check", logx.S("info", baseInfo))

			if isTop {
				isTop = false
				sources = row.Keys()
			} else if !slices.Equal(row.Keys(), sources) {
				// all rows should consist of the same sources
				logx.G().Error("Join: Inconsistent rows", logx.II("want", sources), logx.II("got", row.Keys()), logx.S("info", baseInfo))
				return
			}

			// whether the row already contains the sources of the relation or not
			lRow, lExist := row[lKey.Src]
			rRow, rExist := row[rKey.Src]
			switch {
			case lExist && !rExist:
				lScanned, err := lIndex.Read(lRow.Item())
				if err != nil {
					logx.G().Debug("Join: left read", logx.Err(err), logx.S("info", baseInfo))
					continue
				}
				key, err := lIndex.KeyFunc()(lScanned.Line())
				if err != nil {
					logx.G().Debug("Join: left key", logx.Err(err), logx.S("info", baseInfo))
					continue
				}
				rItemList, ok := rIndex.Get(key)
				if !ok {
					continue
				}
				for _, rItem := range rItemList {
					l := row.Clone()
					l.Set(NewSelectItem(rKey.Src, rItem))
					logx.G().Debug("Join: from left",
						logx.Group("left", logx.Any("row", lRow), logx.S("line", lScanned.Line())),
						logx.S("key", key),
						logx.Group("right", logx.Any("item", rItem)),
						logx.S("info", baseInfo),
					)
					resultC <- l
				}
			case !lExist && rExist:
				rScanned, err := rIndex.Read(rRow.Item())
				if err != nil {
					logx.G().Debug("Join: right read", logx.Err(err), logx.S("info", baseInfo))
					continue
				}
				key, err := rIndex.KeyFunc()(rScanned.Line())
				if err != nil {
					logx.G().Debug("Join: right key", logx.Err(err), logx.S("info", baseInfo))
					continue
				}
				lItemList, ok := lIndex.Get(key)
				if !ok {
					continue
				}
				for _, lItem := range lItemList {
					l := row.Clone()
					l.Set(NewSelectItem(lKey.Src, lItem))
					logx.G().Debug("Join: from right",
						logx.Group("right", logx.Any("row", rRow), logx.S("line", rScanned.Line())),
						logx.S("key", key),
						logx.Group("left", logx.Any("item", lItem)),
						logx.S("info", baseInfo),
					)
					resultC <- l
				}
			case lExist && rExist:
				lScanned, err := lIndex.Read(lRow.Item())
				if err != nil {
					logx.G().Debug("Join: row left read", logx.Err(err), logx.S("info", baseInfo))
					continue
				}
				lk, err := lIndex.KeyFunc()(lScanned.Line())
				if err != nil {
					logx.G().Debug("Join: row left key", logx.Err(err), logx.S("info", baseInfo))
					continue
				}
				rScanned, err := rIndex.Read(rRow.Item())
				if err != nil {
					logx.G().Debug("Join: row right read", logx.Err(err), logx.S("info", baseInfo))
					continue
				}
				rk, err := rIndex.KeyFunc()(rScanned.Line())
				if err != nil {
					logx.G().Debug("Join: row right key", logx.Err(err), logx.S("info", baseInfo))
					continue
				}
				logx.G().Debug("Join: row",
					logx.Group("left",
						logx.Any("row", lRow),
						logx.S("line", lScanned.Line()),
						logx.S("key", lk),
					),
					logx.Group("right",
						logx.Any("row", rRow),
						logx.S("line", rScanned.Line()),
						logx.S("key", rk),
					),
				)
				if lk == rk {
					resultC <- row
				}
			default:
				logx.G().Warn("Join: no rows found", logx.S("info", baseInfo))
			}
		}
	}()
	return resultC
}

type Joiner interface {
	Join(ctx context.Context, key *joinkey.JoinKey) <-chan SelectItemList
}

func New(relJoiner RelationJoiner) Joiner {
	return &joinerImpl{
		relJoiner: relJoiner,
	}
}

type joinerImpl struct {
	relJoiner RelationJoiner
}

func (j *joinerImpl) Join(ctx context.Context, key *joinkey.JoinKey) <-chan SelectItemList {
	if len(key.RelationList) == 0 {
		logx.G().Error("Joiner: empty key")
		resultC := make(chan SelectItemList)
		close(resultC)
		return resultC
	}

	var resultC <-chan SelectItemList
	for _, k := range key.RelationList {
		resultC = j.relJoiner.Join(ctx, k, resultC)
	}
	return resultC
}
