package joinkey_test

import (
	"bytes"
	"testing"

	"github.com/berquerant/joiny/cc/joinkey"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	for _, tc := range []struct {
		title string
		input string
		want  *joinkey.JoinKey
	}{
		{
			title: "single key",
			input: "1.2=2.3",
			want: joinkey.NewJoinKey([]*joinkey.Relation{
				joinkey.NewRelation(
					joinkey.NewLocation(1, 2),
					joinkey.NewLocation(2, 3),
				),
			}),
		},
		{
			title: "double keys",
			input: "1.2=2.3,3.1=1.4",
			want: joinkey.NewJoinKey([]*joinkey.Relation{
				joinkey.NewRelation(
					joinkey.NewLocation(1, 2),
					joinkey.NewLocation(2, 3),
				),
				joinkey.NewRelation(
					joinkey.NewLocation(3, 1),
					joinkey.NewLocation(1, 4),
				),
			}),
		},
	} {
		tc := tc
		t.Run(tc.title, func(t *testing.T) {
			lex := joinkey.NewLexer(bytes.NewBufferString(tc.input))
			_ = joinkey.Parse(lex)
			assert.Nil(t, lex.Err())
			assert.Equal(t, "", cmp.Diff(tc.want, lex.JoinKey))
		})
	}
}
