package cache

import (
	"testing"
	"time"

	"github.com/coreos/etcd/integration"
	"golang.org/x/net/context"
	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/testapi"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/storage"
	"k8s.io/kubernetes/pkg/storage/versioner"
)

func TestCachePut(t *testing.T) {
	s, _, cleanup := testCacheSetup(t)
	defer cleanup()

	getObj, err := s.Get("/pods/foo")
	if err != nil {
		t.Fatal(err)
	}
	if getObj != nil {
		t.Errorf("expect nil object, get = %v", getObj)
	}

	obj := &api.Pod{}
	obj.Name = "foo"
	_, err = s.Put("/pods/foo", obj, 0)
	if err != nil {
		t.Fatal(err)
	}
	getObj, err = s.Get("/pods/foo")
	if err != nil {
		t.Fatal(err)
	}

	if getObj == nil || getObj.(*api.Pod).Name != "foo" {
		t.Error("'foo' pod should have been put. But get unexpected result.")
	}
}

func TestCacheDelete(t *testing.T) {
	s, _, cleanup := testCacheSetup(t)
	defer cleanup()

	obj := &api.Pod{}
	obj.Name = "foo"
	putObj, err := s.Put("/pods/foo", obj, 0)
	if err != nil {
		t.Fatal(err)
	}
	rev, err := versioner.GlobalVersioner.ObjectResourceVersion(putObj)
	if err != nil {
		t.Fatal(err)
	}
	delObj, err := s.Delete("/pods/foo", int64(rev))
	if err != nil {
		t.Fatal(err)
	}

	if delObj == nil || delObj.(*api.Pod).Name != "foo" {
		t.Error("'foo' pod should have been deleted and returned. But get unexpected result.")
	}
}

func TestCacheList(t *testing.T) {
	s, _, cleanup := testCacheSetup(t)
	defer cleanup()

	obj := &api.Pod{}
	obj.Name = "foo"
	_, err := s.Put("/pods/foo", obj, 0)
	if err != nil {
		t.Fatal(err)
	}
	obj.Name = "bar"
	curObj, err := s.Put("/pods/bar", obj, 0)
	if err != nil {
		t.Fatal(err)
	}
	rev, err := versioner.GlobalVersioner.ObjectResourceVersion(curObj)
	if err != nil {
		t.Fatal(err)
	}
	objects, _, err := s.List("/pods/", int64(rev))
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 2 {
		t.Error("Expect both 'foo' and 'bar' pods")
	}
}

func TestCacheWatch(t *testing.T) {
	s, ctx, cleanup := testCacheSetup(t)
	defer cleanup()

	wch, _ := s.WatchPrefix(ctx, "/", 0)
	testCacheWatch(t, s, wch)
}

func TestCacheDirectWatch(t *testing.T) {
	s, ctx, cleanup := testCacheSetup(t)
	defer cleanup()

	wch, _ := s.WatchPrefix(ctx, "/", 1)
	testCacheWatch(t, s, wch)
}

func testCacheWatch(t *testing.T, s *store2, wch storage.WatchChan) {
	obj := &api.Pod{}
	obj.Name = "foo"
	_, err := s.Put("/pods/foo", obj, 0)
	if err != nil {
		t.Fatal(err)
	}
	var res runtime.Object
	select {
	case r := <-wch:
		res = r.Object
	case <-time.After(1 * time.Second):
		t.Fatal("timeout")
	}

	if res == nil || res.(*api.Pod).Name != "foo" {
		t.Error("'foo' pod should have been put. But get unexpected result.")
	}
}

func testCacheSetup(t *testing.T) (*store2, context.Context, func()) {
	//client, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:2379"}})
	//if err != nil {
	//	t.Fatal(err)
	//}
	//cleanupFunc := func() {
	//  cancel()
	//	client.Delete(context.Background(), "/", clientv3.WithPrefix())
	//	client.Close()
	//}
	//return client, cleanupFunc

	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	client := cluster.RandClient()

	s := newStore2(client, testapi.Default.Codec(), "/", "")
	ctx, cancel := context.WithCancel(context.Background())
	go s.run(ctx)

	cleanupFunc := func() {
		cancel()
		cluster.Terminate(t)
	}

	return s, ctx, cleanupFunc
}
