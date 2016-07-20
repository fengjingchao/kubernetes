/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package factory

import (
	"strings"

	"github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/storage"
	"k8s.io/kubernetes/pkg/storage/cache"
	"k8s.io/kubernetes/pkg/storage/etcd3"
	"k8s.io/kubernetes/pkg/storage/storagebackend"
)

// Create creates a storage backend based on given config.
func Create(c storagebackend.Config, codec runtime.Codec, resourcePrefix string) (storage.Interface, error) {
	return newStorage2(c, codec, resourcePrefix)

	// switch c.Type {
	// case storagebackend.StorageTypeUnset, storagebackend.StorageTypeETCD2:
	// 	return newETCD2Storage(c, codec)
	// case storagebackend.StorageTypeETCD3:
	// 	// TODO: We have the following features to implement:
	// 	// - Support secure connection by using key, cert, and CA files.
	// 	// - Honor "https" scheme to support secure connection in gRPC.
	// 	// - Support non-quorum read.
	// 	return newETCD3Storage(c, codec)
	// default:
	// 	return nil, fmt.Errorf("unknown storage type: %s", c.Type)
	// }
}

func newStorage2(c storagebackend.Config, codec runtime.Codec, resourcePrefix string) (storage.Interface, error) {
	endpoints := c.ServerList
	for i, s := range endpoints {
		endpoints[i] = strings.TrimLeft(s, "http://")
	}
	cfg := clientv3.Config{
		Endpoints: endpoints,
	}
	client, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}
	etcd3.StartCompactor(context.Background(), client)
	return cache.New(client, codec, resourcePrefix+"/", c.Prefix), nil
}
