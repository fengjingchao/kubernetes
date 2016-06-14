package framework

import (
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/watch"
)

type ctrlEventType string

const (
	eventTypeRelist ctrlEventType = "RELIST"
	eventTypeAdd    ctrlEventType = ctrlEventType(watch.Added)
	eventTypeModify ctrlEventType = ctrlEventType(watch.Modified)
	eventTypeDelete ctrlEventType = ctrlEventType(watch.Deleted)
	eventTypeResync ctrlEventType = "RESYNC"
)

type ctrlEvent struct {
	eventType ctrlEventType
	items     []runtime.Object
	obj       runtime.Object
}
