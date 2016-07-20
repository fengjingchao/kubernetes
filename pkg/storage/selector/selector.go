package selector

import (
	"k8s.io/kubernetes/pkg/labels"
	"k8s.io/kubernetes/pkg/runtime"
)

type FieldValueGetFunc func(field string, obj runtime.Object) (string, bool)

type Selector struct {
	Op     labels.Operator // TODO: define storage layer its own selector
	Field  string
	Values []string
	//
	FVGetFunc FieldValueGetFunc
}

func IsSelected(obj runtime.Object, ss []Selector) bool {
	for _, s := range ss {
		if !singleSelected(obj, s) {
			return false
		}
	}
	return true
}

func singleSelected(obj runtime.Object, s Selector) bool {
	switch s.Op {
	case labels.InOperator, labels.EqualsOperator, labels.DoubleEqualsOperator:
		v, ok := s.FVGetFunc(s.Field, obj)
		if !ok {
			return false
		}
		return has(s.Values, v)
	case labels.NotInOperator, labels.NotEqualsOperator:
		v, ok := s.FVGetFunc(s.Field, obj)
		if !ok {
			return true
		}
		return !has(s.Values, v)
	case labels.ExistsOperator:
		// The field would be like "pod.labelkey.resource"
		// FVGetFunc will return ok=true if the key exists.
		_, ok := s.FVGetFunc(s.Field, obj)
		return ok
	case labels.DoesNotExistOperator:
		_, ok := s.FVGetFunc(s.Field, obj)
		return !ok
	// TODO: GT, LT
	default:
		panic("")
	}
}

func has(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
