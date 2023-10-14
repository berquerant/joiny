package target

import (
	"fmt"
	"math"
)

type Node interface {
	IsNode()
}

//go:generate go run github.com/berquerant/marker@v0.1.4 -method IsNode -type Location,Single,Left,Right,Interval,Target -output ast_marker_node_generated.go

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

type Range interface {
	IsRange()
	// Ends returns the left-closed right-opened interval.
	Ends() (*Location, *Location)
}

//go:generate go run github.com/berquerant/marker@v0.1.4 -method IsRange -type Single,Left,Right,Interval -output ast_marker_range_generated.go

// Single column.
type Single struct {
	Loc *Location
}

func (s *Single) Ends() (*Location, *Location) {
	return s.Loc, s.Loc.Add(1, 1)
}

func (s *Single) String() string { return fmt.Sprintf("Single(%v)", s.Loc) }

func NewSingle(loc *Location) *Single {
	return &Single{
		Loc: loc,
	}
}

// Left limited range.
type Left struct {
	Loc *Location
}

func (l *Left) String() string { return fmt.Sprintf("Left(%v)", l.Loc) }

func (l *Left) Ends() (*Location, *Location) {
	return l.Loc, &Location{
		Src: l.Loc.Src + 1,
		Col: math.MaxInt,
	}
}

func NewLeft(loc *Location) *Left {
	return &Left{
		Loc: loc,
	}
}

// Right limited range.
type Right struct {
	Loc *Location
}

func (r *Right) String() string { return fmt.Sprintf("Right(%v)", r.Loc) }

func (r *Right) Ends() (*Location, *Location) {
	return &Location{
		Src: r.Loc.Src,
		Col: 1,
	}, r.Loc.Add(1, 1)
}

func NewRight(loc *Location) *Right {
	return &Right{
		Loc: loc,
	}
}

// Interval is the inclusive interval.
type Interval struct {
	Left  *Location
	Right *Location
}

func (i *Interval) String() string { return fmt.Sprintf("Interval(%v, %v)", i.Left, i.Right) }

func (i *Interval) Ends() (*Location, *Location) {
	return i.Left, i.Right.Add(1, 1)
}

func NewInterval(left, right *Location) *Interval {
	return &Interval{
		Left:  left,
		Right: right,
	}
}

type Target struct {
	RangeList []Range
}

func NewTarget(rangeList []Range) *Target {
	return &Target{
		RangeList: rangeList,
	}
}
