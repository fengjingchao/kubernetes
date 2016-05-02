#!/bin/bash

# Copyright 2015 The Kubernetes Authors All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# TODO: figure out how to get etcd tag from some real configuration and put it here.

# Increase the allowed number of open file descriptors
ulimit -n 65536

tar xzf kubernetes-server-linux-amd64.tar.gz

kubernetes/server/bin/kube-scheduler --master=127.0.0.1:8080 --v=2 &> /var/log/kube-scheduler.log &

kubernetes/server/bin/kube-apiserver \
	--portal-net=10.0.0.1/24 \
	--address=0.0.0.0 \
	--etcd-servers=etcdv3-single:4001 \
	--etcd-servers-overrides=/events#etcdv3-single-event:4001 \
	--v=4 \
	--tls-cert-file=/srv/kubernetes/server.cert \
	--tls-private-key-file=/srv/kubernetes/server.key \
	--client-ca-file=/srv/kubernetes/ca.crt \
	--token-auth-file=/srv/kubernetes/known_tokens.csv \
	--secure-port=443 \
	--basic-auth-file=/srv/kubernetes/basic_auth.csv \
	--service-cluster-ip-range=10.0.0.0/16 \
	--delete-collection-workers=16 &> /var/log/kube-apiserver.log &

# kube-contoller-manager now needs running kube-api server to actually start
until [ "$(curl 127.0.0.1:8080/healthz 2> /dev/null)" == "ok" ]; do
	sleep 1
done
kubernetes/server/bin/kube-controller-manager --master=127.0.0.1:8080 --service-account-private-key-file=/srv/kubernetes/server.key --root-ca-file=/srv/kubernetes/ca.crt --v=2 &> /var/log/kube-controller-manager.log &
