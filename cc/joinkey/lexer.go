package joinkey

import (
	"io"
	"strconv"
	"unicode"

	"github.com/berquerant/logger"
	"github.com/berquerant/ybase"
)

func Parse(lexer *Lexer) int {
	logger.G().Debug("Begin parse joinkey")
	defer func() {
		logger.G().Debug("End parse joinkey: %v", lexer.JoinKey)
	}()
	return yyParse(lexer)
}

func ScanToken(r ybase.Reader) int {
	r.DiscardWhile(unicode.IsSpace)
	switch r.Peek() {
	case '=':
		_ = r.Next()
		return EQUAL
	case '.':
		_ = r.Next()
		return DOT
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
	JoinKey *JoinKey
}

func NewLexer(r io.Reader) *Lexer {
	yyErrorVerbose = true
	return &Lexer{
		Lexer: ybase.NewLexer(ybase.NewScanner(
			ybase.NewReader(r, logger.G().Debug),
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
		logger.G().SetLevel(logger.Ldebug)
		yyDebug = level
	case level == 0:
		logger.G().SetLevel(logger.Linfo)
	}
}
