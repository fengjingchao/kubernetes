package versioner

import "k8s.io/kubernetes/pkg/storage/etcd"

var GlobalVersioner = etcd.APIObjectVersioner{}
