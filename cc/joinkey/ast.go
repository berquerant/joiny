package joinkey

import "fmt"

type Node interface {
	IsNode()
}

//go:generate go run github.com/berquerant/marker@v0.1.4 -method IsNode -type Location,Relation,JoinKey -output ast_marker_generated.go

// Location means the specified column of the specified source.
type Location struct {
	Src int
	Col int
}

func (l *Location) Add(src, col int) *Location {
	return &Location{
		Src: l.Src + src,
		Col: l.Col + col,
	}
}

func (l *Location) Source() int    { return l.Src }
func (l *Location) Column() int    { return l.Col }
func (l *Location) String() string { return fmt.Sprintf("Location(%d, %d)", l.Src, l.Col) }

func NewLocation(src, col int) *Location {
	return &Location{
		Src: src,
		Col: col,
	}
}

// Relation means that like sql `join on Left = Right`
type Relation struct {
	Left  *Location
	Right *Location
}

func NewRelation(left, right *Location) *Relation {
	return &Relation{
		Left:  left,
		Right: right,
	}
}

func (r *Relation) String() string { return fmt.Sprintf("Relation(%v, %s)", r.Left, r.Right) }

type JoinKey struct {
	RelationList []*Relation
}

func NewJoinKey(list []*Relation) *JoinKey {
	return &JoinKey{
		RelationList: list,
	}
}
