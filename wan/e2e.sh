e2e.test --host="127.0.0.1:8080" --provider="local" --ginkgo.v=true \
  --kubeconfig="$HOME/.kube/config" --repo-root="$(pwd)" \
  --ginkgo.focus="should\sallow\sstarting\s3\spods\sper\snode"
  #--ginkgo.focus="validates resource limits of pods that are allowed to run"
