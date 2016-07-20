package cache

import (
	"testing"
	"time"

	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/unversioned"
	"k8s.io/kubernetes/pkg/labels"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/storage/selector"
)

func TestWatchIndex(t *testing.T) {
	fooPod := makePod("foo",
		map[string]string{
			"test": "foo",
		})
	barPod := makePod("bar",
		map[string]string{
			"test": "bar",
		})

	noLabelPod := makePod("bar",
		map[string]string{})

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

	tests := []struct {
		ev            *event
		ss            []selector.Selector
		expectWatched bool
	}{{ // Watch(pod.label.test=foo)
		// event: pod[test=foo]
		ev: &event{
			key:       "/",
			obj:       fooPod,
			prevObj:   nil,
			isCreated: true,
			isDeleted: false,
		},
		ss: []selector.Selector{{
			Op:        labels.EqualsOperator,
			Field:     field,
			Values:    []string{"foo"},
			FVGetFunc: g,
		}},
		expectWatched: true,
	}, { // Watch()
		// event: pod[test=foo]
		ev: &event{
			key:       "/",
			obj:       fooPod,
			prevObj:   nil,
			isCreated: true,
			isDeleted: false,
		},
		ss:            []selector.Selector{},
		expectWatched: true,
	}, { // Watch(pod.label.test=foo)
		// event: pod[test=bar]
		ev: &event{
			key:       "/",
			obj:       barPod,
			prevObj:   nil,
			isCreated: true,
			isDeleted: false,
		},
		ss: []selector.Selector{{
			Op:        labels.EqualsOperator,
			Field:     field,
			Values:    []string{"foo"},
			FVGetFunc: g,
		}},
		expectWatched: false,
	}, { // Watch(pod.label.test=foo)
		// event: pod[]
		ev: &event{
			key:       "/",
			obj:       noLabelPod,
			prevObj:   nil,
			isCreated: true,
			isDeleted: false,
		},
		ss: []selector.Selector{{
			Op:        labels.EqualsOperator,
			Field:     field,
			Values:    []string{"foo"},
			FVGetFunc: g,
		}},
		expectWatched: false,
	}}

	timeout := 500 * time.Millisecond

	for i, tt := range tests {
		s := newStore()
		s.AddWatcherIndex(field+".idx", field, g)
		w, err := s.WatchPrefix("/", 0, tt.ss...)
		if err != nil {
			t.Fatal(err)
		}

		s.ReceiveEvent(tt.ev)
		if tt.expectWatched {
			select {
			case <-w.ResultChan():
			case <-time.After(timeout):
				t.Errorf("#%d: timeout after %v", i, timeout)
			}
		} else {
			select {
			case r := <-w.ResultChan():
				t.Errorf("#%d: expected timeout, but get %v", i, r.Object.GetObjectKind().GroupVersionKind())
			case <-time.After(timeout):
			}
		}
		w.Stop()
	}
}

func makePod(name string, label map[string]string) *api.Pod {
	return &api.Pod{
		TypeMeta: unversioned.TypeMeta{
			Kind: "Pod",
		},
		ObjectMeta: api.ObjectMeta{
			Name:   name,
			Labels: label,
		},
	}
}
