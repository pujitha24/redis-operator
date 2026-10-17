package rediscluster

import (
	"context"
	"errors"
	"testing"

	rcvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/rediscluster/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common/redis"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/k8sutils"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeChecker struct {
	redis.Checker
	slotsAssigned bool
	err           error
	called        bool
}

func (f *fakeChecker) CheckClusterSlotsAssigned(ctx context.Context, cr *rcvb2.RedisCluster) (bool, error) {
	f.called = true
	return f.slotsAssigned, f.err
}

func TestShouldScaleUpExistingCluster(t *testing.T) {
	tests := []struct {
		name           string
		leaderCount    int32
		slotsAssigned  bool
		checkErr       error
		wantScaleUp    bool
		wantErr        bool
		wantCheckerHit bool
	}{
		{
			name:           "formed single-node cluster scales up via add-node (issue #1521)",
			leaderCount:    1,
			slotsAssigned:  true,
			wantScaleUp:    true,
			wantCheckerHit: true,
		},
		{
			name:           "fresh nodes without assigned slots fall back to cluster creation",
			leaderCount:    1,
			slotsAssigned:  false,
			wantScaleUp:    false,
			wantCheckerHit: true,
		},
		{
			name:           "formed two-leader cluster scales up via add-node",
			leaderCount:    2,
			slotsAssigned:  true,
			wantScaleUp:    true,
			wantCheckerHit: true,
		},
		{
			name:        "no leaders means initial creation, slot check skipped",
			leaderCount: 0,
			wantScaleUp: false,
		},
		{
			name:        "more than two leaders is always a formed cluster, slot check skipped",
			leaderCount: 3,
			wantScaleUp: true,
		},
		{
			name:           "slot check error is propagated",
			leaderCount:    1,
			checkErr:       errors.New("connection refused"),
			wantErr:        true,
			wantCheckerHit: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checker := &fakeChecker{slotsAssigned: tt.slotsAssigned, err: tt.checkErr}
			r := &Reconciler{Checker: checker}

			scaleUp, err := r.shouldScaleUpExistingCluster(context.Background(), &rcvb2.RedisCluster{}, tt.leaderCount)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantScaleUp, scaleUp)
			}
			assert.Equal(t, tt.wantCheckerHit, checker.called)
		})
	}
}

type fakeStatefulSet struct {
	k8sutils.StatefulSet
	leaderReplicas int32
}

func (f *fakeStatefulSet) GetStatefulSetReplicas(context.Context, string, string) int32 {
	return f.leaderReplicas
}

func (f *fakeStatefulSet) IsStatefulSetReady(context.Context, string, string) bool {
	return false
}

func TestReconcileRefusesDownscaleToZeroLeaders(t *testing.T) {
	zero := int32(0)
	cr := &rcvb2.RedisCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "rc", Namespace: "default"},
		Spec:       rcvb2.RedisClusterSpec{ClusterSize: &zero},
	}
	scheme := runtime.NewScheme()
	assert.NoError(t, rcvb2.AddToScheme(scheme))
	recorder := record.NewFakeRecorder(10)
	r := &Reconciler{
		Client:      fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr).Build(),
		StatefulSet: &fakeStatefulSet{leaderReplicas: 3},
		Recorder:    recorder,
	}

	var (
		res ctrl.Result
		err error
	)
	assert.NotPanics(t, func() {
		res, err = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "rc", Namespace: "default"}})
	})
	assert.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, res)
	select {
	case ev := <-recorder.Events:
		assert.Contains(t, ev, "Warning RedisClusterDownscale")
	default:
		t.Fatal("expected a warning event refusing the downscale")
	}
}
