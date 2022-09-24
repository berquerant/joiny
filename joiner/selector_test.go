package joiner_test

import (
	"context"
	"testing"

	"github.com/berquerant/joiny/cc/target"
	"github.com/berquerant/joiny/joiner"
	"github.com/stretchr/testify/assert"
)

func TestSelectColumnsByRange(t *testing.T) {
	matrix := [][]string{
		{"11", "12", "13"},
		{"21", "22", "23"},
		{"31", "32", "33"},
	}

	for _, tc := range []struct {
		title   string
		rng     target.Range
		sources [][]string
		want    []string
	}{
		{
			title:   "single",
			rng:     target.NewSingle(target.NewLocation(1, 1)),
			sources: matrix,
			want:    []string{"11"},
		},
		{
			title:   "left",
			rng:     target.NewLeft(target.NewLocation(2, 2)),
			sources: matrix,
			want:    []string{"22", "23"},
		},
		{
			title:   "right",
			rng:     target.NewRight(target.NewLocation(3, 2)),
			sources: matrix,
			want:    []string{"31", "32"},
		},
		{
			title:   "interval",
			rng:     target.NewInterval(target.NewLocation(1, 1), target.NewLocation(1, 2)),
			sources: matrix,
			want:    []string{"11", "12"},
		},
		{
			title:   "interval over sources",
			rng:     target.NewInterval(target.NewLocation(1, 1), target.NewLocation(2, 2)),
			sources: matrix,
			want:    []string{"11", "12", "13", "21", "22"},
		},
	} {
		tc := tc
		t.Run(tc.title, func(t *testing.T) {
			got, err := joiner.SelectColumnsByRange(tc.rng, tc.sources)
			assert.Nil(t, err)
			assert.Equal(t, tc.want, got)
		})
	}

	t.Run("out of range", func(t *testing.T) {
		_, err := joiner.SelectColumnsByRange(target.NewSingle(target.NewLocation(10, 1)), matrix)
		assert.ErrorIs(t, err, joiner.ErrOutOfRange)
	})
}

func TestSelectColumnsByTarget(t *testing.T) {
	matrix := [][]string{
		{"11", "12", "13"},
		{"21", "22", "23"},
		{"31", "32", "33"},
	}

	for _, tc := range []struct {
		title   string
		tgt     *target.Target
		sources [][]string
		want    []string
	}{
		{
			title:   "nil target",
			tgt:     target.NewTarget(nil),
			sources: matrix,
			want:    []string{},
		},
		{
			title:   "a range",
			tgt:     target.NewTarget([]target.Range{target.NewSingle(target.NewLocation(1, 1))}),
			sources: matrix,
			want:    []string{"11"},
		},
		{
			title: "sum",
			tgt: target.NewTarget([]target.Range{
				target.NewSingle(target.NewLocation(1, 1)),
				target.NewSingle(target.NewLocation(2, 3)),
				target.NewSingle(target.NewLocation(1, 3)),
			}),
			sources: matrix,
			want:    []string{"11", "23", "13"},
		},
	} {
		tc := tc
		t.Run(tc.title, func(t *testing.T) {
			got, err := joiner.SelectColumnsByTarget(tc.tgt, tc.sources)
			assert.Nil(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

type mockIndex struct {
	v map[string]string
}

func (*mockIndex) KeyFunc() joiner.KeyFunc                          { return nil }
func (*mockIndex) Scan(_ context.Context) <-chan joiner.ScannedItem { return nil }
func (*mockIndex) Get(_ string) ([]joiner.Item, bool)               { return nil, false }
func (*mockIndex) AllItems(_ context.Context) <-chan joiner.Item    { return nil }
func (m *mockIndex) Read(item joiner.Item) (joiner.ScannedItem, error) {
	// find line by key
	return joiner.NewScannedItem(m.v[item.Key()], item), nil
}

type mockCache struct {
	v []joiner.Index
}

func (*mockCache) Delimiter() string                 { return "," }
func (*mockCache) Get(_, _ int) (joiner.Index, bool) { return nil, false }
func (m *mockCache) GetBySrc(src int) ([]joiner.Index, bool) {
	return []joiner.Index{m.v[src]}, true
}

func TestSelector(t *testing.T) {
	data := []map[string]string{
		// source 0
		{
			"11": "111,112,113",
			"12": "121,122,123",
		},
		// source 1
		{
			"21": "211,212,213",
			"22": "221,222,223",
		},
	}

	for _, tc := range []struct {
		title string
		data  []map[string]string
		items []joiner.SelectItem
		tgt   *target.Target
		want  string
	}{
		{
			title: "single source single column",
			data:  data,
			items: []joiner.SelectItem{
				joiner.NewSelectItem(0, joiner.NewItem("11", 0, 0)),
			},
			tgt:  target.NewTarget([]target.Range{target.NewSingle(target.NewLocation(1, 2))}),
			want: "112",
		},
		{
			title: "single source 2 columns",
			data:  data,
			items: []joiner.SelectItem{
				joiner.NewSelectItem(0, joiner.NewItem("11", 0, 0)),
			},
			tgt: target.NewTarget([]target.Range{
				target.NewSingle(target.NewLocation(1, 2)),
				target.NewSingle(target.NewLocation(1, 1)),
			}),
			want: "112,111",
		},
		{
			title: "2 sources 3 columns",
			data:  data,
			items: []joiner.SelectItem{
				joiner.NewSelectItem(0, joiner.NewItem("11", 0, 0)),
				joiner.NewSelectItem(1, joiner.NewItem("22", 0, 0)),
			},
			tgt: target.NewTarget([]target.Range{
				target.NewSingle(target.NewLocation(1, 2)),
				target.NewSingle(target.NewLocation(2, 1)),
				target.NewSingle(target.NewLocation(1, 1)),
			}),
			want: "112,221,111",
		},
	} {
		t.Run(tc.title, func(t *testing.T) {
			indexList := make([]joiner.Index, len(tc.data))
			for i, d := range tc.data {
				indexList[i] = &mockIndex{
					v: d,
				}
			}
			s := joiner.NewSelector(&mockCache{
				v: indexList,
			})
			got, err := s.Select(tc.tgt, tc.items)
			assert.Nil(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
