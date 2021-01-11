package replica_pod_remove_test

import (
	"e2e-basic/common"
	disconnect_lib "e2e-basic/node_disconnect/lib"

	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var env disconnect_lib.DisconnectEnv

const gStorageClass = "mayastor-nvmf-3"

func TestMayastorPodLoss(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Replica pod removal tests")
}

var _ = Describe("Mayastor replica pod removal test", func() {

	It("should define the storage class to use", func() {
		common.MkStorageClass(gStorageClass, 3, "nvmf", "io.openebs.csi-mayastor")
	})

	It("should verify nvmf nexus behaviour when a mayastor pod is removed", func() {
		env = disconnect_lib.Setup("loss-test-pvc-nvmf", gStorageClass, "fio")
		env.PodLossTest()
	})
})

var _ = BeforeSuite(func(done Done) {
	logf.SetLogger(zap.LoggerTo(GinkgoWriter, true))
	common.SetupTestEnv()
	close(done)
}, 60)

var _ = AfterSuite(func() {
	By("tearing down the test environment")

	env.UnsuppressMayastorPod()
	env.Teardown() // removes fio pod and volume

	common.RmStorageClass(gStorageClass)
	common.TeardownTestEnv()
})
