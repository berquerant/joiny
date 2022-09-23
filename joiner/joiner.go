package joiner

import (
	"fmt"
	"sort"

	"github.com/berquerant/joiny/cc/joinkey"
	"github.com/berquerant/logger"
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
	FullJoin(rel *joinkey.Relation) <-chan SelectItemList
	// Join links given rows and the other records.
	// Fallback to FullJoin if rowC is nil.
	Join(rel *joinkey.Relation, rowC <-chan SelectItemList) <-chan SelectItemList
}

func NewRelationJoiner(cache Cache) RelationJoiner {
	return &relationJoiner{
		cache: cache,
	}
}

type relationJoiner struct {
	cache Cache
}

func (r *relationJoiner) FullJoin(rel *joinkey.Relation) <-chan SelectItemList {
	resultC := make(chan SelectItemList, 100)
	go func() {
		defer close(resultC)
		lKey, rKey := rel.Left.Add(-1, -1), rel.Right.Add(-1, -1)
		lIndex, ok := r.cache.Get(lKey.Src, lKey.Col)
		if !ok {
			logger.G().Warn("FullJoin: left index not found: %v", lKey)
			return
		}
		rIndex, ok := r.cache.Get(rKey.Src, rKey.Col)
		if !ok {
			logger.G().Warn("FullJoin: right index not found: %v", rKey)
			return
		}

		// cross join for all items
		for lItem := range lIndex.AllItems() {
			rItemList, ok := rIndex.Get(lItem.Key())
			if !ok {
				continue
			}
			list := make(SelectItemList)
			list.Set(NewSelectItem(lKey.Src, lItem))
			for _, rItem := range rItemList {
				l := list.Clone()
				l.Set(NewSelectItem(rKey.Src, rItem))
				logger.G().Debug("FullJoin: %v %v %v", lKey, rKey, l)
				resultC <- l
			}
		}
	}()
	return resultC
}

func (r *relationJoiner) Join(rel *joinkey.Relation, rowC <-chan SelectItemList) <-chan SelectItemList {
	if rowC == nil {
		return r.FullJoin(rel)
	}

	resultC := make(chan SelectItemList, 100)
	go func() {
		defer close(resultC)

		lKey, rKey := rel.Left.Add(-1, -1), rel.Right.Add(-1, -1)
		lIndex, ok := r.cache.Get(lKey.Src, lKey.Col)
		if !ok {
			logger.G().Warn("Join: left index not found: %v", lKey)
			return
		}
		rIndex, ok := r.cache.Get(rKey.Src, rKey.Col)
		if !ok {
			logger.G().Warn("Join: right index not found: %v", rKey)
			return
		}

		var (
			isTop   = true
			sources []int
		)
		for row := range rowC {
			baseInfo := func() string { return fmt.Sprintf("lkey %v rkey %v row %v", lKey, rKey, row) }
			logger.G().Debug("Join check: %s", baseInfo())

			if isTop {
				isTop = false
				sources = row.Keys()
			} else if !slices.Equal(row.Keys(), sources) {
				logger.G().Error("Join: Insonsistent rows, want %+v got %+v, %s", sources, row.Keys(), baseInfo())
				return
			}

			lRow, lExist := row[lKey.Src]
			rRow, rExist := row[rKey.Src]
			switch {
			case lExist && !rExist:
				lScanned, err := lIndex.Read(lRow.Item())
				if err != nil {
					logger.G().Debug("Join: left read %v, %s", err, baseInfo())
					continue
				}
				key, err := lIndex.KeyFunc()(lScanned.Line())
				if err != nil {
					logger.G().Debug("Join: left key %v, %s", err, baseInfo())
					continue
				}
				rItemList, ok := rIndex.Get(key)
				if !ok {
					continue
				}
				for _, rItem := range rItemList {
					l := row.Clone()
					l.Set(NewSelectItem(rKey.Src, rItem))
					logger.G().Debug("Join: from left lrow %v lline %s key %s ritem %+v, %s",
						lRow, lScanned.Line(), key, rItem, baseInfo(),
					)
					resultC <- l
				}
			case !lExist && rExist:
				rScanned, err := rIndex.Read(rRow.Item())
				if err != nil {
					logger.G().Debug("Join: right read %v, %s", err, baseInfo())
					continue
				}
				key, err := rIndex.KeyFunc()(rScanned.Line())
				if err != nil {
					logger.G().Debug("Join: right key %v, %s", err, baseInfo())
					continue
				}
				lItemList, ok := lIndex.Get(key)
				if !ok {
					continue
				}
				for _, lItem := range lItemList {
					l := row.Clone()
					l.Set(NewSelectItem(lKey.Src, lItem))
					logger.G().Debug("Join: from right rrow %v rline %s key %s litem %+v, %s",
						rRow, rScanned.Line(), key, lItem, baseInfo(),
					)
					resultC <- l
				}
			case lExist && rExist:
				lScanned, err := lIndex.Read(lRow.Item())
				if err != nil {
					logger.G().Debug("Join: row left read %v, %s", err, baseInfo())
					continue
				}
				lk, err := lIndex.KeyFunc()(lScanned.Line())
				if err != nil {
					logger.G().Debug("Join: row left key %v, %s", err, baseInfo())
					continue
				}
				rScanned, err := rIndex.Read(rRow.Item())
				if err != nil {
					logger.G().Debug("Join: row right read %v, %s", err, baseInfo())
					continue
				}
				rk, err := rIndex.KeyFunc()(rScanned.Line())
				if err != nil {
					logger.G().Debug("Join: row right key %v, %s", err, baseInfo())
					continue
				}
				logger.G().Debug("Join: row lrow %v rrow %v lline %s rline %s lk %s rk %s",
					lRow, rRow, lScanned.Line(), rScanned.Line(), lk, rk,
				)
				if lk == rk {
					resultC <- row
				}
			default:
				logger.G().Warn("Join: no rows found, %s", baseInfo())
			}
		}
	}()
	return resultC
}

type Joiner interface {
	Join(key *joinkey.JoinKey) <-chan SelectItemList
}

func New(relJoiner RelationJoiner) Joiner {
	return &joinerImpl{
		relJoiner: relJoiner,
	}
}

type joinerImpl struct {
	relJoiner RelationJoiner
}

func (j *joinerImpl) Join(key *joinkey.JoinKey) <-chan SelectItemList {
	if len(key.RelationList) == 0 {
		logger.G().Error("Joiner: empty key")
		resultC := make(chan SelectItemList)
		close(resultC)
		return resultC
	}

	var resultC <-chan SelectItemList
	for _, k := range key.RelationList {
		resultC = j.relJoiner.Join(k, resultC)
	}
	return resultC
}
