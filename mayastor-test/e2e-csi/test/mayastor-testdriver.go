/*
Copyright 2019 The Kubernetes Authors.
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

package test

import (
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/framework/skipper"
	"k8s.io/kubernetes/test/e2e/storage/testpatterns"
	"k8s.io/kubernetes/test/e2e/storage/testsuites"
)

type mayastorDriver struct {
	driverInfo testsuites.DriverInfo
	manifests  []string
}
var (
	MayastorDriver = InitMayastorDriver
	defaultStorageClassParameters = map[string]string{
		"repl":  "1",
		"protocol": "nvmf",
	}
)

// initMayastorDriver returns mayastorDriver that implements TestDriver interface
func initMayastorDriver(name string, manifests ...string) testsuites.TestDriver {
	return &mayastorDriver{
		driverInfo: testsuites.DriverInfo{
			Name:        name,
			MaxFileSize: testpatterns.FileSizeLarge,
			SupportedFsType: sets.NewString(
				"", // Default fsType
			),
			// Defined in kubernetes/test/e2e/storage/testsuites/testdriver.go
			Capabilities: map[testsuites.Capability]bool{
				testsuites.CapPersistence: true,
				testsuites.CapBlock:       true,
				testsuites.CapExec:        true,
				testsuites.CapMultiPODs:      true,
			},
		},
		manifests: manifests,
	}
}

func InitMayastorDriver() testsuites.TestDriver {
	return initMayastorDriver("csi-mayastorplugin",
		"csi-daemonset.yaml",
		"mayastor-daemonset.yaml",
		"moac-deployment.yaml",
		"moac-rbac.yaml",
		"nats-deployment.yaml",
		"namespace.yaml",
	)
}

var _ testsuites.TestDriver = &mayastorDriver{}

func (n *mayastorDriver) GetDriverInfo() *testsuites.DriverInfo {
	return &n.driverInfo
}

func (n *mayastorDriver) SkipUnsupportedTest(pattern testpatterns.TestPattern) {
	// Defined in kubernetes/test/e2e/storage/testpatterns/testpattern.go
	if pattern.VolType != testpatterns.DynamicPV {
		skipper.Skipf("Unsupported volType -- skipping")
	}
}

func (n *mayastorDriver) PrepareTest(f *framework.Framework) (*testsuites.PerTestConfig, func()) {
	config := &testsuites.PerTestConfig{
		Driver:    n,
		Prefix:    "mayastor",
		Framework: f,
	}

	return config, func() {}
}

// normalizeProvisioner extracts any '/' character in the provisioner name to '-'.
// StorageClass name cannot container '/' character.
func normalizeProvisioner(provisioner string) string {
	return strings.ReplaceAll(provisioner, "/", "-")
}

func getStorageClass(
	generateName string,
	provisioner string,
	parameters map[string]string,
	mountOptions []string,
	reclaimPolicy *v1.PersistentVolumeReclaimPolicy,
	bindingMode *storagev1.VolumeBindingMode,
	allowedTopologies []v1.TopologySelectorTerm,
) *storagev1.StorageClass {
	if reclaimPolicy == nil {
		defaultReclaimPolicy := v1.PersistentVolumeReclaimDelete
		reclaimPolicy = &defaultReclaimPolicy
	}
	if bindingMode == nil {
		defaultBindingMode := storagev1.VolumeBindingImmediate
		bindingMode = &defaultBindingMode
	}
	allowVolumeExpansion := false
	return &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: generateName,
		},
		Provisioner:          provisioner,
		Parameters:           parameters,
		MountOptions:         mountOptions,
		ReclaimPolicy:        reclaimPolicy,
		VolumeBindingMode:    bindingMode,
		AllowedTopologies:    allowedTopologies,
		AllowVolumeExpansion: &allowVolumeExpansion,
	}
}

func (n *mayastorDriver) GetDynamicProvisionStorageClass(config *testsuites.PerTestConfig, fsType string) *storagev1.StorageClass {
	provisioner := "io.openebs.csi-mayastor"
	generateName := fmt.Sprintf("%s-%s-dynamic-sc-", config.DriverNamespace.String(), normalizeProvisioner(provisioner))
	mountOptions := []string{}
	allowedTopologies := []v1.TopologySelectorTerm{}
	return getStorageClass(generateName,
		"io.openebs.csi-mayastor",
		defaultStorageClassParameters,
		mountOptions,
		nil,
		nil,
		allowedTopologies,
		)
}

