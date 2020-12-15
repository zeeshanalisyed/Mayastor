#!/usr/bin/env bash
kubeconfig_file="$HOME/.kube/config"
#go test -v  mayastor-e2e-suite-test.go -ginkgo.v -ginkgo.progress --kubeconfig=$kubeconfigfile -timeout=0
go test -v  mayastor-e2e-suite-test.go -ginkgo.v -ginkgo.progress -timeout=0

