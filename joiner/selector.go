package joiner

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/berquerant/joiny/cc/target"
	"github.com/berquerant/joiny/slicing"
	"github.com/berquerant/logger"
)

var ErrOutOfRange = errors.New("out of range")

func SelectColumnsByRange(rng target.Range, sources [][]string) ([]string, error) {
	left, right := rng.Ends()
	left, right = left.Add(-1, -1), right.Add(-1, -1) // zero-based
	if !(slicing.InRange(sources, left.Src) && slicing.InRange(sources, right.Src-1)) {
		return nil, fmt.Errorf("Select range: %w %v sources len %d", ErrOutOfRange, rng, len(sources))
	}

	srcs := sources[left.Src:right.Src]
	switch len(srcs) {
	case 0:
		return nil, nil
	case 1:
		return slicing.Interval(srcs[0], left.Col, right.Col), nil
	case 2:
		return slicing.Flat(
			slicing.Left(srcs[0], left.Col),
			slicing.Right(srcs[1], right.Col),
		), nil
	default:
		return slicing.Flat(
			slicing.Left(srcs[0], left.Col),
			slicing.Flat(
				slicing.Interval(srcs, 1, right.Src-1)...,
			),
			slicing.Right(srcs[len(srcs)-1], right.Col),
		), nil
	}
}

func SelectColumnsByTarget(tgt *target.Target, sources [][]string) ([]string, error) {
	r := make([][]string, len(tgt.RangeList))
	for i, rng := range tgt.RangeList {
		x, err := SelectColumnsByRange(rng, sources)
		if err != nil {
			return nil, err
		}
		r[i] = x
	}
	return slicing.Flat(r...), nil
}

//go:generate go run github.com/berquerant/dataclass@v0.1.0 -type SelectItem -field "Source int,Item Item" -output selector_dataclass_selectitem_generated.go

type Selector interface {
	// Select forms selected items into a line depending on the target.
	// items specify the data sources, target is columns to be selected.
	Select(tgt *target.Target, items []SelectItem) (string, error)
}

func NewSelector(cache Cache) Selector {
	return &selector{
		cache: cache,
	}
}

type selector struct {
	cache Cache
}

func (s *selector) Select(tgt *target.Target, items []SelectItem) (string, error) {
	items = slicing.Uniq(items, func(item SelectItem) int { return item.Source() })
	sort.Slice(items, func(i, j int) bool { return items[i].Source() < items[j].Source() })

	lines := make([][]string, len(items))
	for i, item := range items {
		srcs, found := s.cache.GetBySrc(item.Source())
		if !found {
			return "", fmt.Errorf("Select: %w %v", ErrOutOfRange, item)
		}
		src := srcs[0]
		scanned, err := src.Read(item.Item())
		if err != nil {
			return "", fmt.Errorf("Select: %w %v", err, item)
		}
		lines[i] = strings.Split(scanned.Line(), s.cache.Delimiter())
	}
	selected, err := SelectColumnsByTarget(tgt, lines)
	if err != nil {
		return "", fmt.Errorf("Select: %w", err)
	}
	logger.G().Debug("Select: %v %v %v return %v", items, tgt, lines, selected)
	return strings.Join(selected, s.cache.Delimiter()), nil
}
