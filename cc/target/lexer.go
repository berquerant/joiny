package target

import (
	"fmt"
	"io"
	"strconv"
	"unicode"

	"github.com/berquerant/joiny/logx"
	"github.com/berquerant/ybase"
)

func Parse(lexer *Lexer) int {
	logx.G().Debug("Begin parse target")
	defer func() {
		logx.G().Debug("End parse target", logx.Any("target", lexer.Target))
	}()
	return yyParse(lexer)
}

func ScanToken(r ybase.Reader) int {
	r.DiscardWhile(unicode.IsSpace)
	switch r.Peek() {
	case '.':
		_ = r.Next()
		return DOT
	case '-':
		_ = r.Next()
		return MINUS
	case ',':
		_ = r.Next()
		return COMMA
	default:
		r.NextWhile(unicode.IsDigit)
		if r.Buffer() == "" {
			return ybase.EOF
		}
		return UINT
	}
}

type Lexer struct {
	ybase.Lexer
	Target *Target
}

func NewLexer(r io.Reader) *Lexer {
	yyErrorVerbose = true
	debug := func(msg string, v ...any) {
		logx.G().Debug(fmt.Sprintf(msg, v...))
	}
	return &Lexer{
		Lexer: ybase.NewLexer(ybase.NewScanner(
			ybase.NewReader(r, debug),
			ScanToken,
		)),
	}
}

func (l *Lexer) ParseUint(value string) uint {
	ui, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		l.Errorf("Cannot parse %s as uint %w", value, err)
		return 0
	}
	return uint(ui)
}

func (l *Lexer) Lex(lval *yySymType) int {
	return l.DoLex(func(tok ybase.Token) {
		lval.token = tok
	})
}

func (*Lexer) Debug(level int) {
	switch {
	case level > 0:
		logx.G().SetLevel(logx.Ldebug)
		yyDebug = level
	case level == 0:
		logx.G().SetLevel(logx.Linfo)
	}
}
