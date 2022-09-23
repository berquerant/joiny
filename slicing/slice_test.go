package slicing_test

import (
	"sort"
	"testing"

	"github.com/berquerant/joiny/slicing"
	"github.com/stretchr/testify/assert"
)

func TestInterval(t *testing.T) {
	for _, tc := range []struct {
		title string
		data  []int
		left  int
		right int
		want  []int
	}{
		{
			title: "normal",
			data:  []int{0, 1, 2},
			left:  0,
			right: 2,
			want:  []int{0, 1},
		},
		{
			title: "max coerced",
			data:  []int{0, 1, 2},
			left:  0,
			right: 100,
			want:  []int{0, 1, 2},
		},
		{
			title: "min coerced",
			data:  []int{0, 1, 2},
			left:  -100,
			right: 3,
			want:  []int{0, 1, 2},
		},
		{
			title: "detect reverse",
			data:  []int{0, 1, 2},
			left:  2,
			right: 1,
		},
	} {
		tc := tc
		t.Run(tc.title, func(t *testing.T) {
			got := slicing.Interval(tc.data, tc.left, tc.right)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestFlat(t *testing.T) {
	for _, tc := range []struct {
		title string
		data  [][]int
		want  []int
	}{
		{
			title: "nil",
			want:  []int{},
		},
		{
			title: "identity",
			data:  [][]int{{1, 2, 3}},
			want:  []int{1, 2, 3},
		},
		{
			title: "join",
			data: [][]int{
				{1, 2, 3},
				{4, 5, 6},
				{7, 8},
			},
			want: []int{1, 2, 3, 4, 5, 6, 7, 8},
		},
	} {
		t.Run(tc.title, func(t *testing.T) {
			got := slicing.Flat(tc.data...)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestUniq(t *testing.T) {
	for _, tc := range []struct {
		title string
		data  []string
		want  []string
		key   func(string) string
	}{
		{
			title: "nil",
			want:  []string{},
			key:   func(v string) string { return v },
		},
		{
			title: "uniq by identity",
			data: []string{
				"a",
				"b",
				"a",
				"c",
			},
			want: []string{
				"a",
				"b",
				"c",
			},
			key: func(v string) string { return v },
		},
	} {
		t.Run(tc.title, func(t *testing.T) {
			got := slicing.Uniq(tc.data, tc.key)
			sort.Strings(got)
			sort.Strings(tc.want)
			assert.Equal(t, tc.want, got)
		})
	}
}
