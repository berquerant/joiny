package target_test

import (
	"bytes"
	"testing"

	"github.com/berquerant/joiny/cc/target"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	for _, tc := range []struct {
		title string
		input string
		want  *target.Target
	}{
		{
			title: "single",
			input: "1.2",
			want: target.NewTarget([]target.Range{
				target.NewSingle(target.NewLocation(1, 2)),
			}),
		},
		{
			title: "left",
			input: "2.1-",
			want: target.NewTarget([]target.Range{
				target.NewLeft(target.NewLocation(2, 1)),
			}),
		},
		{
			title: "right",
			input: "-3.2",
			want: target.NewTarget([]target.Range{
				target.NewRight(target.NewLocation(3, 2)),
			}),
		},
		{
			title: "interval",
			input: "1.2-1.5",
			want: target.NewTarget([]target.Range{
				target.NewInterval(target.NewLocation(1, 2), target.NewLocation(1, 5)),
			}),
		},
		{
			title: "fuzz",
			input: "1.2,2.1-,-3.2,1.2-1.5,1.2-2.5",
			want: target.NewTarget([]target.Range{
				target.NewSingle(target.NewLocation(1, 2)),
				target.NewLeft(target.NewLocation(2, 1)),
				target.NewRight(target.NewLocation(3, 2)),
				target.NewInterval(target.NewLocation(1, 2), target.NewLocation(1, 5)),
				target.NewInterval(target.NewLocation(1, 2), target.NewLocation(2, 5)),
			}),
		},
	} {
		tc := tc
		t.Run(tc.title, func(t *testing.T) {
			lex := target.NewLexer(bytes.NewBufferString(tc.input))
			_ = target.Parse(lex)
			assert.Nil(t, lex.Err())
			assert.Equal(t, "", cmp.Diff(tc.want, lex.Target))
		})
	}
}
