// Code generated by "dataclass -type Item -field Key string|Offset int64|Size int -output index_dataclass_item_generated.go"; DO NOT EDIT.

package joiner

type Item interface {
	Key() string
	Offset() int64
	Size() int
}
type item struct {
	key    string
	offset int64
	size   int
}

func (s *item) Key() string   { return s.key }
func (s *item) Offset() int64 { return s.offset }
func (s *item) Size() int     { return s.size }
func NewItem(
	key string,
	offset int64,
	size int,
) Item {
	return &item{
		key:    key,
		offset: offset,
		size:   size,
	}
}
