package joiner_test

import (
	"sort"
	"strings"
	"testing"

	"github.com/berquerant/joiny/async"
	"github.com/berquerant/joiny/joiner"
	"github.com/berquerant/joiny/temporary"
	"github.com/stretchr/testify/assert"
)

func TestIndex(t *testing.T) {
	const content = `k1 v1
k2 v2
k3 v4
k2 v4
`
	f, err := temporary.NewFile()
	if err != nil {
		t.Fatalf("create tmp file %v", err)
	}
	defer f.Close()
	if _, err := f.Write([]byte(content)); err != nil {
		t.Fatalf("write to tmp file %v", err)
	}
	index, err := joiner.NewIndex(async.NewReadSeeker(f), func(val string) (string, error) {
		return strings.Split(val, " ")[0], nil
	})
	if err != nil {
		t.Fatalf("new index %v", err)
	}

	for _, tc := range []struct {
		title string
		key   string
		want  []string
	}{
		{
			title: "miss",
			key:   "yog",
		},
		{
			title: "hit",
			key:   "k1",
			want: []string{
				"k1 v1",
			},
		},
		{
			title: "2 hits",
			key:   "k2",
			want: []string{
				"k2 v2",
				"k2 v4",
			},
		},
	} {
		tc := tc
		t.Run(tc.title, func(t *testing.T) {
			items, found := index.Get(tc.key)
			if !found {
				assert.Equal(t, 0, len(tc.want))
				return
			}
			got := []string{}
			for _, item := range items {
				scanned, err := index.Read(item)
				if err != nil {
					t.Fatalf("scan %v %v", item, err)
				}
				got = append(got, scanned.Line())
			}
			assert.Equal(t, tc.want, got)
		})
	}

	t.Run("scan", func(t *testing.T) {
		want := []string{
			"k1 v1",
			"k2 v2",
			"k3 v4",
			"k2 v4",
		}
		got := []string{}
		for item := range index.Scan() {
			got = append(got, item.Line())
		}
		sort.Strings(got)
		sort.Strings(want)
		assert.Equal(t, want, got)
	})
}
