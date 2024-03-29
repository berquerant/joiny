// Code generated by "dataclass -type SelectItem -field Source int|Item Item -output selector_dataclass_selectitem_generated.go"; DO NOT EDIT.

package joiner

type SelectItem interface {
	Source() int
	Item() Item
}
type selectItem struct {
	source int
	item   Item
}

func (s *selectItem) Source() int { return s.source }
func (s *selectItem) Item() Item  { return s.item }
func NewSelectItem(
	source int,
	item Item,
) SelectItem {
	return &selectItem{
		source: source,
		item:   item,
	}
}
