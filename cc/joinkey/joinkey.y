%{
package joinkey

import "github.com/berquerant/ybase"
%}

%union{
  location *Location
  relation_list []*Relation
  relation *Relation
  joinkey *JoinKey
  token ybase.Token
}

%type <joinkey> joinkey
%type <relation_list> relation_list
%type <relation> relation
%type <location> location

%token <token> UINT
%token <token> EQUAL
%token <token> DOT
%token <token> COMMA

%%

joinkey:
  relation_list {
    r := NewJoinKey($1)
    yylex.(*Lexer).JoinKey = r
    $$ = r
  }

relation_list:
  relation {
    $$ = []*Relation{$1}
  }
  | relation_list COMMA relation {
    $$ = append($1, $3)
  }

relation:
  location EQUAL location {
    $$ = NewRelation($1, $3)
  }

location:
  UINT DOT UINT {
    lex := yylex.(*Lexer)
    left := int(lex.ParseUint($1.Value()))
    right := int(lex.ParseUint($3.Value()))
    $$ = NewLocation(left, right)
  }
