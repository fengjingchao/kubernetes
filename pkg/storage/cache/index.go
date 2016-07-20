package cache

import (
	"k8s.io/kubernetes/pkg/storage/selector"
)

type fieldIndex struct {
	FVGetFunc selector.FieldValueGetFunc
	// Note: we could use btree for generic cases. Currently assumes only equality cases.
	index map[string]map[string]struct{}
}

func newFieldIndex(g selector.FieldValueGetFunc) *fieldIndex {
	return &fieldIndex{
		FVGetFunc: g,
		index:     make(map[string]map[string]struct{}),
	}
}

func (fi *fieldIndex) put(fieldValue string, pointer string) {
	s, ok := fi.index[fieldValue]
	if !ok {
		s = make(map[string]struct{})
		fi.index[fieldValue] = s
	}
	s[pointer] = struct{}{}
}
