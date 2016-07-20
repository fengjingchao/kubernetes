package cache

import (
	"testing"

	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/labels"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/storage/selector"
)

func BenchmarkWatcherIndex(b *testing.B) {
	benchmarkWatcherIndex(b, true)
}

func BenchmarkNoWatcherIndex(b *testing.B) {
	benchmarkWatcherIndex(b, false)
}

func benchmarkWatcherIndex(b *testing.B, withIndex bool) {
	const maxWatcherNum = 100000
	s := newStore()
	g := func(field string, obj runtime.Object) (string, bool) {
		pod, ok := obj.(*api.Pod)
		if !ok {
			return "", false
		}
		switch field {
		case "pod.label.test":
			return pod.Labels["test"], true
		default:
			panic("")
		}
	}
	field := "pod.label.test"
	if withIndex {
		s.AddWatcherIndex(field+".idx", field, g)
	}
	fooPod := makePod("foo",
		map[string]string{
			"test": "foo",
		})
	ev := &event{
		key:       "/",
		obj:       fooPod,
		prevObj:   nil,
		isCreated: true,
		isDeleted: false,
	}
	ss := []selector.Selector{{
		Op:        labels.EqualsOperator,
		Field:     field,
		Values:    []string{"foo"},
		FVGetFunc: g,
	}}
	// Have 10 interested in fooPod and the rest isn't
	for i := 0; i < 10; i++ {
		s.createWatcher("/", 0, ss)
	}
	ss[0].Values = []string{"bar"}
	for i := 10; i < maxWatcherNum; i++ {
		s.createWatcher("/", 0, ss)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if withIndex {
			s.goThruIndex(ev)
		} else {
			s.goThruAll(ev)
		}
	}
}
