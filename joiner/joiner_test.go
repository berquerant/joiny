package joiner_test

import (
	"context"
	"io"
	"sort"
	"strings"
	"testing"

	"github.com/berquerant/joiny/cc/joinkey"
	"github.com/berquerant/joiny/cc/target"
	"github.com/berquerant/joiny/joiner"
	"github.com/berquerant/joiny/temporary"
	"github.com/berquerant/logger"
	"github.com/stretchr/testify/assert"
)

type multiSourceGenerator struct {
	rows     []string
	fileList temporary.FileList
}

func (m *multiSourceGenerator) add(row string) { m.rows = append(m.rows, row) }
func (m *multiSourceGenerator) close()         { m.fileList.Close() }

func (m *multiSourceGenerator) generate() {
	r, err := temporary.NewFileList(len(strings.Split(m.rows[0], "|")))
	if err != nil {
		panic(err)
	}
	for i := range r {
		f, err := temporary.NewFile()
		if err != nil {
			panic(err)
		}
		r[i] = f
	}
	for _, row := range m.rows {
		xs := strings.Split(row, "|")
		for i, x := range xs {
			r[i].WriteString(x + "\n")
		}
	}
	m.fileList = r
}

func (m *multiSourceGenerator) readSeekers() []io.ReadSeeker {
	r := make([]io.ReadSeeker, len(m.fileList))
	for i, f := range m.fileList {
		r[i] = f
	}
	return r
}

func TestJoiner(t *testing.T) {
	if testing.Verbose() {
		logger.G().SetLevel(logger.Ltrace)
		defer logger.G().SetLevel(logger.Linfo)
	}

	t.Run("full join", func(t *testing.T) {
		for _, tc := range []struct {
			title string
			rows  []string
			rel   *joinkey.Relation
			tgt   *target.Target
			want  []string
		}{
			{
				title: "single col",
				rows: []string{
					"a,b,c|d,e,f",
					"p,x,y|p,z,t",
				},
				rel: joinkey.NewRelation(joinkey.NewLocation(1, 1), joinkey.NewLocation(2, 1)),
				tgt: target.NewTarget([]target.Range{
					target.NewSingle(target.NewLocation(1, 2)),
				}),
				want: []string{
					"x",
				},
			},
			{
				title: "multiple cols",
				rows: []string{
					"a,b,c|d,e,f",
					"p,x,y|p,z,t",
				},
				rel: joinkey.NewRelation(joinkey.NewLocation(1, 1), joinkey.NewLocation(2, 1)),
				tgt: target.NewTarget([]target.Range{
					target.NewSingle(target.NewLocation(1, 2)),
					target.NewSingle(target.NewLocation(1, 3)),
					target.NewSingle(target.NewLocation(2, 2)),
				}),
				want: []string{
					"x,y,z",
				},
			},
			{
				title: "multiple cols dup",
				rows: []string{
					"a,b,c|d,e,f",
					"p,x,y|p,z,t",
				},
				rel: joinkey.NewRelation(joinkey.NewLocation(1, 1), joinkey.NewLocation(2, 1)),
				tgt: target.NewTarget([]target.Range{
					target.NewSingle(target.NewLocation(1, 2)),
					target.NewSingle(target.NewLocation(1, 3)),
					target.NewSingle(target.NewLocation(2, 2)),
					target.NewSingle(target.NewLocation(1, 2)),
				}),
				want: []string{
					"x,y,z,x",
				},
			},
			{
				title: "multiple rows",
				rows: []string{
					"11,12,13|14,15,16",
					"21,22,23|11,25,26",
					"31,32,33|11,35,36",
					"14,42,43|44,45,46",
				},
				rel: joinkey.NewRelation(joinkey.NewLocation(1, 1), joinkey.NewLocation(2, 1)),
				tgt: target.NewTarget([]target.Range{
					target.NewSingle(target.NewLocation(1, 1)),
					target.NewSingle(target.NewLocation(1, 3)),
					target.NewSingle(target.NewLocation(2, 3)),
				}),
				want: []string{
					"11,13,26",
					"11,13,36",
					"14,43,16",
				},
			},
		} {
			t.Run(tc.title, func(t *testing.T) {
				g := &multiSourceGenerator{}
				for _, r := range tc.rows {
					g.add(r)
				}
				g.generate()
				defer g.close()

				cache, err := joiner.NewCacheBuilder(
					g.readSeekers(),
					joiner.RelationListToLocationList([]*joinkey.Relation{
						tc.rel,
					}),
					",",
				).Build(context.TODO())
				if err != nil {
					t.Fatal(err)
				}

				j := joiner.NewRelationJoiner(cache)
				s := joiner.NewSelector(cache)
				got := []string{}
				for x := range j.FullJoin(context.TODO(), tc.rel) {
					v, err := s.Select(tc.tgt, x.Sorted())
					if err != nil {
						t.Fatal(err)
					}
					got = append(got, v)
				}
				sort.Strings(got)
				sort.Strings(tc.want)
				assert.Equal(t, tc.want, got)
			})
		}
	})

	t.Run("join", func(t *testing.T) {
		for _, tc := range []struct {
			title string
			rows  []string
			key   *joinkey.JoinKey
			tgt   *target.Target
			want  []string
		}{
			{
				title: "single col",
				rows: []string{
					"a,b,c|d,e,f",
					"p,x,y|p,z,t",
				},
				key: joinkey.NewJoinKey([]*joinkey.Relation{
					joinkey.NewRelation(joinkey.NewLocation(1, 1), joinkey.NewLocation(2, 1)),
				}),
				tgt: target.NewTarget([]target.Range{
					target.NewSingle(target.NewLocation(1, 2)),
				}),
				want: []string{
					"x",
				},
			},
			{
				title: "key dup",
				rows: []string{
					"a,b,c|d,e,f",
					"p,x,y|p,z,t",
				},
				key: joinkey.NewJoinKey([]*joinkey.Relation{
					joinkey.NewRelation(joinkey.NewLocation(1, 1), joinkey.NewLocation(2, 1)),
					joinkey.NewRelation(joinkey.NewLocation(1, 1), joinkey.NewLocation(2, 1)),
				}),
				tgt: target.NewTarget([]target.Range{
					target.NewSingle(target.NewLocation(1, 2)),
				}),
				want: []string{
					"x",
				},
			},
			{
				title: "2 joins",
				rows: []string{
					"a,b,c|d,e,f|p,q,r",
					"p,x,y|p,z,t|p,q2,r2",
				},
				key: joinkey.NewJoinKey([]*joinkey.Relation{
					joinkey.NewRelation(joinkey.NewLocation(1, 1), joinkey.NewLocation(2, 1)),
					joinkey.NewRelation(joinkey.NewLocation(2, 1), joinkey.NewLocation(3, 1)),
				}),
				tgt: target.NewTarget([]target.Range{
					target.NewSingle(target.NewLocation(1, 2)),
					target.NewSingle(target.NewLocation(3, 2)),
				}),
				want: []string{
					"x,q",
					"x,q2",
				},
			},
			{
				title: "2 joins multiple",
				rows: []string{
					"11,12|13,14|15,16,17",
					"21,22|11,24|25,26,27",
					"31,32|21,34|11,36,37",
					"41,42|43,44|11,46,47",
					"11,52|53,54|55,56,57",
				},
				key: joinkey.NewJoinKey([]*joinkey.Relation{
					joinkey.NewRelation(joinkey.NewLocation(1, 1), joinkey.NewLocation(2, 1)),
					joinkey.NewRelation(joinkey.NewLocation(2, 1), joinkey.NewLocation(3, 1)),
				}),
				tgt: target.NewTarget([]target.Range{
					target.NewSingle(target.NewLocation(1, 2)),
					target.NewSingle(target.NewLocation(2, 2)),
					target.NewSingle(target.NewLocation(3, 2)),
				}),
				want: []string{
					"12,24,36",
					"12,24,46",
					"52,24,36",
					"52,24,46",
				},
			},
			{
				title: "3 joins",
				rows: []string{
					"11,12|13,14|15,16,17|18.19",
					"21,22|11,24|25,26,27|11,29",
					"31,32|21,34|11,36,37|38,39",
					"41,42|43,44|11,46,47|48,49",
					"11,52|53,54|55,56,57|58,59",
				},
				key: joinkey.NewJoinKey([]*joinkey.Relation{
					joinkey.NewRelation(joinkey.NewLocation(1, 1), joinkey.NewLocation(2, 1)),
					joinkey.NewRelation(joinkey.NewLocation(2, 1), joinkey.NewLocation(3, 1)),
					joinkey.NewRelation(joinkey.NewLocation(1, 1), joinkey.NewLocation(4, 1)),
				}),
				tgt: target.NewTarget([]target.Range{
					target.NewSingle(target.NewLocation(1, 2)),
					target.NewSingle(target.NewLocation(2, 2)),
					target.NewSingle(target.NewLocation(3, 2)),
					target.NewSingle(target.NewLocation(4, 2)),
				}),
				want: []string{
					"12,24,36,29",
					"12,24,46,29",
					"52,24,36,29",
					"52,24,46,29",
				},
			},
			{
				title: "3 joins with internal join",
				rows: []string{
					"11,12|13,14|15,16,17",
					"21,22|11,11|25,26,27",
					"31,32|21,34|11,36,37",
					"41,42|43,44|11,46,47",
					"11,52|53,54|55,56,57",
				},
				key: joinkey.NewJoinKey([]*joinkey.Relation{
					joinkey.NewRelation(joinkey.NewLocation(1, 1), joinkey.NewLocation(2, 1)),
					joinkey.NewRelation(joinkey.NewLocation(2, 1), joinkey.NewLocation(3, 1)),
					joinkey.NewRelation(joinkey.NewLocation(2, 2), joinkey.NewLocation(1, 1)),
				}),
				tgt: target.NewTarget([]target.Range{
					target.NewSingle(target.NewLocation(1, 2)),
					target.NewSingle(target.NewLocation(2, 2)),
					target.NewSingle(target.NewLocation(3, 2)),
				}),
				want: []string{
					"12,11,36",
					"12,11,46",
					"52,11,36",
					"52,11,46",
				},
			},
		} {
			t.Run(tc.title, func(t *testing.T) {
				g := &multiSourceGenerator{}
				for _, r := range tc.rows {
					g.add(r)
				}
				g.generate()
				defer g.close()

				cache, err := joiner.NewCacheBuilder(
					g.readSeekers(),
					joiner.RelationListToLocationList(tc.key.RelationList),
					",",
				).Build(context.TODO())
				if err != nil {
					t.Fatal(err)
				}

				s := joiner.NewSelector(cache)
				j := joiner.New(joiner.NewRelationJoiner(cache))
				got := []string{}
				for x := range j.Join(context.TODO(), tc.key) {
					v, err := s.Select(tc.tgt, x.Sorted())
					if err != nil {
						t.Fatal(err)
					}
					got = append(got, v)
				}
				sort.Strings(got)
				sort.Strings(tc.want)
				assert.Equal(t, tc.want, got)
			})
		}
	})
}
