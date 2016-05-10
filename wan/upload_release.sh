#!/usr/bin/env bash

source './cluster/gce/util.sh'

PROJECT=coreos-k8s-scale-testing ZONE=us-central1-b \
  INSTANCE_PREFIX=e2e-test-ubuntu \
  SERVER_BINARY_TAR=./_output/release-tars/kubernetes-server-linux-amd64.tar.gz \
  SALT_TAR=./_output/release-tars/kubernetes-salt.tar.gz \
  upload-server-tars
