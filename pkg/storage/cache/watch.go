package cache

import (
	"strings"
	"sync"

	"k8s.io/kubernetes/pkg/labels"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/storage/selector"
	"k8s.io/kubernetes/pkg/watch"
)

// Note: indexing watcher
//
// Pod [env=production,RC=xyz]
//
// w1 [env=production,RC=abc]
// w2 [env=production,RC=xyz]
// w3 [env=testing,RC=xyz]
// w4 [env=production]
// w5 [RC=xyz]
// w6 []
//
// Answer: w2, w4, w5, w6
// Select ... Where ('env' = 'production' or 'env' = nil)
//              and ('RC' = 'xyz' or 'RC' = nil)

// pod.env.idx, pod.RC.idx
// 1. iterate all idx getter func on input
// 2. join all results; or get best results in an index and then do filtering.
//    This is *select* details.

type store struct {
	sync.RWMutex

	// We need to put watch stuff here because we need to lock on both map and watch event queue.
	globalWatcherPrimaryKey int

	watchers       map[int]*watcher
	watcherIndexes map[string]*watcherIndex

	// rotating queue for watch events. Note: Hack.
	events     []*event
	startIndex int
	endIndex   int
}

func newStore() *store {
	return &store{
		watchers:       make(map[int]*watcher),
		watcherIndexes: make(map[string]*watcherIndex),
	}
}

func (s *store) AddWatcherIndex(idxName, field string, g selector.FieldValueGetFunc) error {
	s.Lock()
	defer s.Unlock()

	s.watcherIndexes[field] = newWatcherIndex(g)

	// fmt.Println("AddWatcherIndex:", field)

	if len(s.watchers) > 0 {
		panic("reindex")
	}
	return nil
}

func (s *store) WatchPrefix(key string, rev int64, ss ...selector.Selector) (watch.Interface, error) {
	s.Lock()
	defer s.Unlock()

	// Inside impl, we have different semantics from outside storage layer.
	// Instead of watch "after" given revision, it's watch "at".
	if rev != 0 {
		rev = rev + 1
	}

	return s.createWatcher(key, rev, ss)
}

func (s *store) createWatcher(key string, rev int64, ss []selector.Selector) (*watcher, error) {
	if rev != 0 {
		panic("TODO...")
	}

	// TODO: We need to watch at rev here before updating index. If error we can return so.
	//       Ideally, we should pass watch chan into the watcher constructor.

	w := newWatcher(key, ss)

	pk := s.globalWatcherPrimaryKey
	s.globalWatcherPrimaryKey++
	s.watchers[pk] = w
	s.updateWatcherIndex(pk, w)

	go func() {
		w.run()

		s.Lock()
		defer s.Unlock()
		s.deleteWatcher(pk)
	}()

	return w, nil
}

func (s *store) updateWatcherIndex(pk int, w *watcher) {
	for field, idx := range s.watcherIndexes {
		indexed := false
		for _, sl := range w.ss {
			if sl.Field == field {
				indexed = true
				// fmt.Println("updateWatcherIndex:", field, sl.Values[0])
				idx.add(pk, sl)
				break
			}
		}
		if indexed {
			continue
		}
		// TODO: hack..

		// fmt.Println("updateWatcherIndex:", field, "nil")
		idx.add(pk, selector.Selector{
			Op:     labels.EqualsOperator,
			Field:  field,
			Values: []string{""},
		})
	}
}

func (s *store) deleteWatcher(pk int) {
	delete(s.watchers, pk)
	// TODO: delete index
}

func (s *store) ReceiveEvent(ev *event) {
	s.Lock()
	defer s.Unlock()

	if len(s.watcherIndexes) != 1 {
		panic("") // TODO: let's just handle one index case.
	}

	if s.goThruIndex(ev) {
		return
	}

	if len(s.watcherIndexes) > 0 {
		panic("")
	}
	// No indexes, fall back to scanning all watchers.
	s.goThruAll(ev)
}

func (s *store) goThruIndex(ev *event) bool {
	// use indexes to find watcher
	for field, idx := range s.watcherIndexes {
		// TODO: hack on not checking nil and use "" instead..
		sentEmpty := false
		curVal, _ := idx.g(field, ev.obj)
		// fmt.Println("cur", curVal)
		s.dispatchEvent(ev, idx, curVal)
		if curVal == "" {
			sentEmpty = true
		}
		if ev.prevObj != nil {
			prevVal, _ := idx.g(field, ev.prevObj)
			if prevVal != curVal {
				s.dispatchEvent(ev, idx, prevVal)
			}
			if prevVal == "" {
				sentEmpty = true
			}
		}
		if !sentEmpty {
			s.dispatchEvent(ev, idx, "")
		}
	}
	return true
}

func (s *store) goThruAll(ev *event) {
	for _, w := range s.watchers {
		w.handle(ev)
	}
}

func (s *store) dispatchEvent(ev *event, idx *watcherIndex, value string) {
	// TODO: let's just handle '=' case
	for _, pk := range idx.eqs[value] {
		// fmt.Println("dispatchEvent:", pk, value)
		w := s.watchers[pk]
		// Note: We only have watchPrefix..
		// If this assumption isn't true anymore, we need to do exact match.
		if strings.HasPrefix(ev.key, w.key) {
			w.handle(ev)
		}
	}
}

type event struct {
	key       string
	obj       runtime.Object
	prevObj   runtime.Object
	isDeleted bool
	isCreated bool
}

// Implements watch.Interface
type watcher struct {
	key string
	ss  []selector.Selector

	eventCh  chan *event
	resultCh chan watch.Event
	stopCh   chan struct{}
}

func newWatcher(key string, ss []selector.Selector) *watcher {
	return &watcher{
		key:      key,
		ss:       ss,
		eventCh:  make(chan *event, 100),
		resultCh: make(chan watch.Event, 100),
		stopCh:   make(chan struct{}),
	}
}

func (w *watcher) Stop() {
	close(w.stopCh)
	// Wait for complete deletion (index) here?
}

func (w *watcher) ResultChan() <-chan watch.Event {
	return w.resultCh
}

func (w *watcher) handle(ev *event) {
	w.eventCh <- ev
}

func (w *watcher) run() {
	// Total hack.. Assuming everything is nonblocking..
	for {
		select {
		case ev := <-w.eventCh:
			res := transform(ev, w.ss)
			if res == nil {
				break
			}
			// fmt.Println("event:", res.Type)
			w.resultCh <- *res
		case <-w.stopCh:
			return
		}
	}
}

// TODO: This is stupid. It's implemented again and again.
func transform(ev *event, ss []selector.Selector) (res *watch.Event) {
	switch {
	case ev.isDeleted:
		if !filter(ev.prevObj, ss) {
			return nil
		}
		res = &watch.Event{
			Type:   watch.Deleted,
			Object: ev.prevObj,
		}
	case ev.isCreated:
		if !filter(ev.obj, ss) {
			return nil
		}
		res = &watch.Event{
			Type:   watch.Added,
			Object: ev.obj,
		}
	default:
		curObjPasses := filter(ev.obj, ss)
		oldObjPasses := filter(ev.prevObj, ss)
		switch {
		case curObjPasses && oldObjPasses:
			res = &watch.Event{
				Type:   watch.Modified,
				Object: ev.obj,
			}
		case curObjPasses && !oldObjPasses:
			res = &watch.Event{
				Type:   watch.Added,
				Object: ev.obj,
			}
		case !curObjPasses && oldObjPasses:
			res = &watch.Event{
				Type:   watch.Deleted,
				Object: ev.prevObj,
			}
		}
	}
	return res
}

type watcherIndex struct {
	eqs BTree

	g selector.FieldValueGetFunc
}

func newWatcherIndex(g selector.FieldValueGetFunc) *watcherIndex {
	return &watcherIndex{
		eqs: BTree{},
		g:   g,
	}
}

func (i *watcherIndex) add(pk int, s selector.Selector) {
	if s.Op != labels.EqualsOperator {
		panic("TODO...")
	}
	for _, v := range s.Values {
		l := i.eqs[v]
		l = append(l, pk)
		i.eqs[v] = l
	}
}

// Note: Hack
type BTree map[string][]int

func filter(obj runtime.Object, ss []selector.Selector) bool {
	for _, s := range ss {
		if !matchSelector(obj, s) {
			return false
		}
	}
	return true
}

func matchSelector(obj runtime.Object, s selector.Selector) bool {
	switch s.Op {
	case labels.EqualsOperator:
		fieldValue, ok := s.FVGetFunc(s.Field, obj)
		if !ok {
			return false
		}
		if !has(s.Values, fieldValue) {
			return false
		}
		return true
	// TODO: more operators..
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
