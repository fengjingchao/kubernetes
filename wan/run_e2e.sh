#!/usr/bin/env bash

export GINKGO_PARALLEL="y"
targs="--ginkgo.skip=\[Slow\]|\[Serial\]|\[Disruptive\]|\[Flaky\]|\[Feature:.+\]"
# targs="--ginkgo.focus=\[Slow\] --ginkgo.skip=\[Serial\]|\[Disruptive\]|\[Flaky\]|\[Feature:.+\]"


# export GINKGO_PARALLEL="n"
# targs="--ginkgo.focus=\[Serial\] --ginkgo.skip=\[Disruptive\]|\[Flaky\]|\[Feature:.+\]"

if [ -z "$1" ]; then
  echo "saving logs to ./e2e.log"
  go run hack/e2e.go -v -test --test_args="${targs}" --check_version_skew=false | tee e2e.log
else
  go run hack/e2e.go -v -test --test_args="${targs}" --check_version_skew=false | tee "$1"
fi
