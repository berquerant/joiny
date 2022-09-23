%{
package target

import "github.com/berquerant/ybase"
%}

%union{
  rnge Range
  location *Location
  target *Target
  token ybase.Token
  range_list []Range
}

%type <location> location
%type <rnge> range
%type <range_list> range_list
%type <target> target

%token <token> UINT
%token <token> DOT
%token <token> MINUS
%token <token> COMMA

%%

target:
  range_list {
    r := NewTarget($1)
    yylex.(*Lexer).Target = r
    $$ = r
  }

range_list:
  range {
    $$ = []Range{$1}
  }
  | range_list COMMA range {
    $$ = append($1, $3)
  }

range:
  location MINUS location {
    $$ = NewInterval($1, $3)
  }
  | MINUS location {
    $$ = NewRight($2)
  }
  | location MINUS {
    $$ = NewLeft($1)
  }
  | location {
    $$ = NewSingle($1)
  }

location:
  UINT DOT UINT {
    lex := yylex.(*Lexer)
    left := int(lex.ParseUint($1.Value()))
    right := int(lex.ParseUint($3.Value()))
    $$ = NewLocation(left, right)
  }
