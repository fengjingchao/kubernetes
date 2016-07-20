package cache

import (
	"fmt"
	"path"
	"strings"
	"sync"

	"github.com/coreos/etcd/clientv3"
	"github.com/golang/glog"
	"golang.org/x/net/context"
	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/storage"
	"k8s.io/kubernetes/pkg/storage/selector"
	"k8s.io/kubernetes/pkg/storage/versioner"
	"k8s.io/kubernetes/pkg/watch"
)

// Note: all keys will be like "${pathPrefix}/${resourcePrefix}/XXX"
type store2 struct {
	// User can specify "etcd-prefix". We need to take of that to all keys passed to storage.
	pathPrefix string

	// The current arch design is that each registry will handle one resource.
	// Each will take care of one specific resource.
	// We will cache such resource objects in storage layer.
	resourcePrefix string

	// resource specific codec
	codec runtime.Codec

	client *clientv3.Client

	// locking and things that need locking
	mu           sync.Mutex
	cond         sync.Cond
	localRev     int64 // max mod rev seen in write op by this store
	globalRev    int64 // global revision seen from big watcher
	objects      map[string]runtime.Object
	watchGroups  map[string]watchGroup
	fieldIndexes map[string]*fieldIndex // field name -> index
}

func New(c *clientv3.Client, codec runtime.Codec, resourcePrefix, pathPrefix string) storage.Interface {
	s := newStore2(c, codec, resourcePrefix, pathPrefix)
	go s.run(context.Background())
	return s
}

func newStore2(client *clientv3.Client, codec runtime.Codec, resourcePrefix, pathPrefix string) *store2 {
	s := &store2{
		resourcePrefix: resourcePrefix,
		pathPrefix:     pathPrefix,
		client:         client,
		codec:          codec,
		objects:        map[string]runtime.Object{},
		watchGroups:    map[string]watchGroup{},
		fieldIndexes:   map[string]*fieldIndex{},
	}
	s.cond.L = &s.mu
	return s
}

func (s *store2) Put(key string, obj runtime.Object, prevVersion int64) (runtime.Object, error) {
	key = s.prefix(key)
	b, err := runtime.Encode(s.codec, obj)
	if err != nil {
		return nil, err
	}

	if prevVersion < 0 {
		resp, err := s.client.KV.Put(context.TODO(), key, string(b))
		if err != nil {
			return nil, err
		}
		cur, err := api.Scheme.Copy(obj)
		if err != nil {
			return nil, err
		}
		err = versioner.GlobalVersioner.UpdateObject(cur, uint64(resp.Header.Revision))
		if err != nil {
			return nil, err
		}
		s.updateLocalRev(resp.Header.Revision)
		return cur, nil
	}

	txnResp, err := s.client.KV.Txn(context.TODO()).If(
		clientv3.Compare(clientv3.ModRevision(key), "=", prevVersion),
	).Then(
		clientv3.OpPut(key, string(b)),
	).Else(
		clientv3.OpGet(key),
	).Commit()
	if err != nil {
		return nil, err
	}

	if !txnResp.Succeeded {
		var ex runtime.Object
		if prevVersion > 0 {
			getResp := txnResp.Responses[0].GetResponseRange()
			ex, err = decode(s.codec, getResp.Kvs[0].Value, getResp.Kvs[0].ModRevision)
			if err != nil {
				return nil, err
			}
		}
		return ex, storage.NewResourceVersionConflictsError(key, prevVersion)
	}

	cur, err := api.Scheme.Copy(obj)
	if err != nil {
		return nil, err
	}
	err = versioner.GlobalVersioner.UpdateObject(cur, uint64(txnResp.Header.Revision))
	if err != nil {
		return nil, err
	}
	s.updateLocalRev(txnResp.Header.Revision)
	return cur, nil
}

func (s *store2) Delete(key string, prevVersion int64) (runtime.Object, error) {
	if prevVersion == 0 {
		return nil, fmt.Errorf("Unexpected parameter value: prevVersion=%d", prevVersion)
	}
	key = s.prefix(key)

	if prevVersion < 0 {
		txnResp, err := s.client.KV.Txn(context.TODO()).If().Then(
			clientv3.OpGet(key),
			clientv3.OpDelete(key),
		).Commit()
		if err != nil {
			return nil, err
		}

		s.updateLocalRev(txnResp.Header.Revision)
		getResp := txnResp.Responses[0].GetResponseRange()
		if len(getResp.Kvs) == 0 {
			return nil, nil
		}
		return decode(s.codec, getResp.Kvs[0].Value, getResp.Kvs[0].ModRevision)
	}

	txnResp, err := s.client.KV.Txn(context.TODO()).If(
		clientv3.Compare(clientv3.ModRevision(key), "=", prevVersion),
	).Then(
		clientv3.OpGet(key),
		clientv3.OpDelete(key),
	).Else(
		clientv3.OpGet(key),
	).Commit()
	if err != nil {
		return nil, err
	}

	getResp := txnResp.Responses[0].GetResponseRange()
	ex, err := decode(s.codec, getResp.Kvs[0].Value, getResp.Kvs[0].ModRevision)
	if err != nil {
		return nil, err
	}

	if !txnResp.Succeeded {
		err = storage.NewResourceVersionConflictsError(key, prevVersion)
		return ex, err
	}

	s.updateLocalRev(txnResp.Header.Revision)
	return ex, nil
}

func (s *store2) Get(key string) (runtime.Object, error) {
	key = s.prefix(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	for s.globalRev < s.localRev {
		s.cond.Wait()
	}

	return s.objects[key], nil
}

func (s *store2) List(prefix string, version int64, ss ...selector.Selector) ([]runtime.Object, int64, error) {
	prefix = s.prefix(prefix)
	s.mu.Lock()
	defer s.mu.Unlock()

	if version > 0 {
		for s.globalRev < version {
			s.cond.Wait()
		}
	}

	res := []runtime.Object{}
	for k, obj := range s.objects {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if !selector.IsSelected(obj, ss) {
			continue
		}
		res = append(res, obj)
	}

	return res, s.globalRev, nil
}

func (s *store2) WatchPrefix(ctx context.Context, prefix string, version int64, ss ...selector.Selector) (storage.WatchChan, error) {
	prefix = s.prefix(prefix)
	if version > 0 {
		wch := s.client.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(version+1))
		w := newDirectWatcher(s, wch, ss)
		go w.run(ctx)
		return w.resultCh, nil
	}

	w := newWatcher2(ctx, ss)
	s.mu.Lock()
	if s.watchGroups[prefix] == nil {
		s.watchGroups[prefix] = watchGroup{}
	}
	s.watchGroups[prefix].Add(w)
	s.mu.Unlock()

	go func() {
		w.run()

		s.mu.Lock()
		s.watchGroups[prefix].Remove(w)
		s.mu.Unlock()
	}()

	return w.resultCh, nil
}

func (s *store2) AddIndex(idxName, field string, g selector.FieldValueGetFunc) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.fieldIndexes[field]; ok {
		panic("TODO: Duplicate index")
	}

	s.fieldIndexes[field] = newFieldIndex(g)
	for k, o := range s.objects {
		v, fieldExist := g(field, o)
		if !fieldExist {
			continue
		}
		s.fieldIndexes[field].put(v, k)
	}
	return nil
}

func (s *store2) DeleteIndex(idxName string) { panic("unimplemented") }

func (s *store2) run(ctx context.Context) {
	watchCh := s.client.Watch(ctx, s.prefix(s.resourcePrefix), clientv3.WithPrefix())
	s.watchLoop(watchCh)

	glog.Infof("unexpected!!! store2 watch loop stopped")

	s.stopAllWatchers()
}

func (s *store2) stopAllWatchers() {
	// TODO: assumes it didn't stop now.
}

func (s *store2) broadcast(ev *event2) {
	for p, g := range s.watchGroups {
		if strings.HasPrefix(string(ev.ETCDEvent.Kv.Key), p) {
			g.Broadcast(ev)
		}
	}
}

func (s *store2) put(key string, obj runtime.Object) {
	s.objects[key] = obj

	// update object index
	for field, index := range s.fieldIndexes {
		v, ok := index.FVGetFunc(field, obj)
		if !ok {
			continue
		}
		index.put(v, key)
	}

	// update watcher index
}

func (s *store2) watchLoop(watchCh clientv3.WatchChan) {
	for resp := range watchCh {
		if resp.Err() != nil {
			// TODO: Return error event before closing.
			glog.Errorf("watch error: %v", resp.Err())
			return
		}
		for _, e := range resp.Events {
			switch e.Type {
			case clientv3.EventTypePut:
				obj, err := decode(s.codec, e.Kv.Value, e.Kv.ModRevision)
				if err != nil {
					panic(err)
				}

				s.mu.Lock()
				prevObj := s.objects[string(e.Kv.Key)]
				// s.objects[string(e.Kv.Key)] = obj
				s.put(string(e.Kv.Key), obj)
				s.broadcast(&event2{
					ETCDEvent:  e,
					Object:     obj,
					PrevObject: prevObj,
				})
				s.mu.Unlock()
			case clientv3.EventTypeDelete:
				s.mu.Lock()
				prevObj := s.objects[string(e.Kv.Key)]
				delete(s.objects, string(e.Kv.Key))
				s.broadcast(&event2{
					ETCDEvent:  e,
					PrevObject: prevObj,
				})
				s.mu.Unlock()
			default:
				glog.Fatalf("Unknown etcd event type (%v)", e.Type)
			}
		}
		s.updateGlobalRev(resp.Events)
	}
}

func (s *store2) updateGlobalRev(events []*clientv3.Event) {
	l := len(events)
	if l == 0 {
		return
	}
	maxModRev := events[l-1].Kv.ModRevision
	s.mu.Lock()
	if maxModRev <= s.globalRev {
		s.mu.Unlock()
		return
	}
	s.globalRev = maxModRev
	s.mu.Unlock()
	s.cond.Broadcast()
}

func (s *store2) updateLocalRev(rev int64) {
	s.mu.Lock()
	if rev > s.localRev {
		s.localRev = rev
	}
	s.mu.Unlock()
}

func (s *store2) prefix(key string) string {
	if len(s.pathPrefix) == 0 {
		return key
	}
	suf := ""
	if strings.HasSuffix(key, "/") {
		suf = "/" // path.Join will lose this suffix
	}
	return path.Join(s.pathPrefix, key) + suf
}

type watchGroup map[*watcher2]struct{}

func (g watchGroup) Add(w *watcher2) {
	g[w] = struct{}{}
}

func (g watchGroup) Remove(w *watcher2) {
	delete(g, w)
}

func (g watchGroup) Broadcast(ev *event2) {
	for w := range g {
		w.OnReceive(ev)
	}
}

type watcher2 struct {
	ctx      context.Context
	resultCh chan storage.WatchResponse
	eventCh  chan *event2
	ss       []selector.Selector
}

func newWatcher2(ctx context.Context, ss []selector.Selector) *watcher2 {
	return &watcher2{
		ctx:      ctx,
		resultCh: make(chan storage.WatchResponse, 100),
		eventCh:  make(chan *event2, 100),
		ss:       ss,
	}
}

func (w *watcher2) OnReceive(ev *event2) {
	switch ev.ETCDEvent.Type {
	case clientv3.EventTypePut:
		if !selector.IsSelected(ev.Object, w.ss) && !selector.IsSelected(ev.PrevObject, w.ss) {
			return
		}
	case clientv3.EventTypeDelete:
		if !selector.IsSelected(ev.PrevObject, w.ss) {
			return
		}
	}
	select {
	case w.eventCh <- ev:
	case <-w.ctx.Done():
	}
}

func (w *watcher2) run() {
	// TODO: handle slow watcher.
	for {
		select {
		case ev := <-w.eventCh:
			r := eventToResponse(ev)
			select {
			case w.resultCh <- r:
			case <-w.ctx.Done():
				return
			}
		case <-w.ctx.Done():
			return
		}
	}
}

// directWatcher is used to watch from etcd directly.
// This is for watchers with older revision.
// TODO: Once it catches up the big watcher, it should join then.
type directWatcher struct {
	wch      clientv3.WatchChan
	s        *store2
	resultCh chan storage.WatchResponse
	ss       []selector.Selector
}

func newDirectWatcher(s *store2, wch clientv3.WatchChan, ss []selector.Selector) *directWatcher {
	return &directWatcher{
		wch:      wch,
		s:        s,
		resultCh: make(chan storage.WatchResponse, 100),
		ss:       ss,
	}
}

func (w *directWatcher) run(ctx context.Context) {
	for resp := range w.wch {
		if resp.Err() != nil {
			// TODO: Return error event before closing.
			glog.Errorf("watch error: %v", resp.Err())
			return
		}
		for _, e := range resp.Events {
			ev := &event2{ETCDEvent: e}
			interested := true

			switch e.Type {
			case clientv3.EventTypePut:
				obj, err := decode(w.s.codec, e.Kv.Value, e.Kv.ModRevision)
				if err != nil {
					panic(err)
				}
				ev.Object = obj
				prevObj, err := getPrevObject(w.s.codec, w.s.client, ctx, string(e.Kv.Key), e.Kv.ModRevision)
				if err != nil {
					panic(err)
				}
				ev.PrevObject = prevObj
				if !selector.IsSelected(obj, w.ss) && !selector.IsSelected(prevObj, w.ss) {
					interested = false
				}
			case clientv3.EventTypeDelete:
				prevObj, err := getPrevObject(w.s.codec, w.s.client, ctx, string(e.Kv.Key), e.Kv.ModRevision)
				if err != nil {
					panic(err)
				}
				ev.PrevObject = prevObj
				if !selector.IsSelected(prevObj, w.ss) {
					interested = false
				}
			default:
				panic("")
			}

			if !interested {
				continue
			}
			r := eventToResponse(ev)
			select {
			case w.resultCh <- r:
			case <-ctx.Done():
			}
		}
	}
}

type event2 struct {
	ETCDEvent  *clientv3.Event
	Object     runtime.Object
	PrevObject runtime.Object
}

func eventToResponse(ev *event2) storage.WatchResponse {
	var typ watch.EventType
	switch ev.ETCDEvent.Type {
	case clientv3.EventTypePut:
		if ev.ETCDEvent.IsCreate() {
			typ = watch.Added
			break
		}
		typ = watch.Modified
	case clientv3.EventTypeDelete:
		typ = watch.Deleted
	}
	r := storage.WatchResponse{
		Type:       typ,
		Object:     ev.Object,
		PrevObject: ev.PrevObject,
	}
	return r
}

func getPrevObject(codec runtime.Codec, client *clientv3.Client, ctx context.Context, key string, curRev int64) (runtime.Object, error) {
	resp, err := client.Get(ctx, key, clientv3.WithRev(curRev-1))
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	return decode(codec, resp.Kvs[0].Value, resp.Kvs[0].ModRevision)
}

func decode(codec runtime.Codec, b []byte, v int64) (runtime.Object, error) {
	obj, err := runtime.Decode(codec, b)
	if err != nil {
		return nil, err
	}
	err = versioner.GlobalVersioner.UpdateObject(obj, uint64(v))
	if err != nil {
		return nil, err
	}
	return obj, nil
}
