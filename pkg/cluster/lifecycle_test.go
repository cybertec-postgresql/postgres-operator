package cluster

import (
	"context"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	acidv1 "github.com/zalando/postgres-operator/pkg/apis/acid.zalan.do/v1"
	fakeacidv1 "github.com/zalando/postgres-operator/pkg/generated/clientset/versioned/fake"
	"github.com/zalando/postgres-operator/pkg/util/config"
	"github.com/zalando/postgres-operator/pkg/util/k8sutil"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
)

var lifecycleLogger = logrus.New().WithField("test", "lifecycle")
var lifecycleEventRecorder = record.NewFakeRecorder(10)

func int32Ptr(i int32) *int32 { return &i }

func newTestPoolerObjects(role PostgresRole, replicas int32) *ConnectionPoolerObjects {
	return &ConnectionPoolerObjects{
		Deployment: &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("test-%s-pooler", role),
				Namespace: "default",
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: int32Ptr(replicas),
			},
		},
		Name:        fmt.Sprintf("test-%s-pooler", role),
		ClusterName: "test-cluster",
		Namespace:   "default",
		Role:        role,
	}
}

func newFakeK8sClientForLifecycle() (*k8sutil.KubernetesClient, *fake.Clientset, *fakeacidv1.Clientset) {
	clientSet := fake.NewSimpleClientset()
	acidClientSet := fakeacidv1.NewSimpleClientset()

	client := &k8sutil.KubernetesClient{
		DeploymentsGetter:   clientSet.AppsV1(),
		PostgresqlsGetter:   acidClientSet.AcidV1(),
		StatefulSetsGetter:  clientSet.AppsV1(),
		ServicesGetter:      clientSet.CoreV1(),
		SecretsGetter:       clientSet.CoreV1(),
		ConfigMapsGetter:    clientSet.CoreV1(),
		PodsGetter:          clientSet.CoreV1(),
		EndpointsGetter:     clientSet.CoreV1(),
		CronJobsGetter:      clientSet.BatchV1(),
	}

	return client, clientSet, acidClientSet
}

// newLifecycleCluster builds a Cluster with the given current status and spec
// snapshot. The Postgres CR is pre-created in the fake clientset so K8s API
// calls work without a separate Create step.
func newLifecycleCluster(
	client *k8sutil.KubernetesClient,
	status string,
	numberOfInstances int32,
	lifecyclePhase string,
	previousNumberOfInstances int32,
	previousPoolerInstances map[string]int32,
) *Cluster {
	pg := &acidv1.Postgresql{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: acidv1.PostgresSpec{
			TeamID:            "test-team",
			NumberOfInstances: numberOfInstances,
			Volume:            acidv1.Volume{Size: "1Gi"},
		},
		Status: acidv1.PostgresStatus{
			PostgresClusterStatus: status,
		},
	}
	if lifecyclePhase != "" {
		pg.Spec.Lifecycle = &acidv1.LifecycleSpec{Phase: lifecyclePhase}
	}
	if previousNumberOfInstances > 0 {
		pg.Status.PreviousNumberOfInstances = previousNumberOfInstances
	}
	if previousPoolerInstances != nil {
		pg.Status.PreviousPoolerInstances = previousPoolerInstances
	}

	created, err := client.Postgresqls("default").Create(context.TODO(), pg, metav1.CreateOptions{})
	if err != nil {
		panic(fmt.Sprintf("failed to pre-create Postgresql: %v", err))
	}

	return &Cluster{
		Config: Config{
			OpConfig: config.Config{
				PodManagementPolicy:    "ordered_ready",
				LogicalBackup: config.LogicalBackup{
					LogicalBackupJobPrefix: "logical-backup-",
				},
			},
		},
		Postgresql:    *created,
		KubeClient:    *client,
		logger:        lifecycleLogger,
		eventRecorder: lifecycleEventRecorder,
	}
}

func TestDetectLifecycleTransition(t *testing.T) {
	tests := []struct {
		name                         string
		currentStatus                string
		newLifecyclePhase            string // "" means nil lifecycle
		newNumberOfInstances         int32
		newPreviousNumberOfInstances int32
		want                         LifecycleAction
	}{
		{
			name:                         "Running + lifecycle.phase=stopped -> Hibernate",
			currentStatus:                acidv1.ClusterStatusRunning,
			newLifecyclePhase:            "stopped",
			newNumberOfInstances:         3,
			newPreviousNumberOfInstances: 0,
			want:                         LifecycleActionHibernate,
		},
		{
			name:                         "Running + no lifecycle -> None",
			currentStatus:                acidv1.ClusterStatusRunning,
			newLifecyclePhase:            "",
			newNumberOfInstances:         3,
			newPreviousNumberOfInstances: 0,
			want:                         LifecycleActionNone,
		},
		{
			name:                         "Stopping + lifecycle.phase=stopped -> None (already stopping)",
			currentStatus:                acidv1.ClusterStatusStopping,
			newLifecyclePhase:            "stopped",
			newNumberOfInstances:         0,
			newPreviousNumberOfInstances: 3,
			want:                         LifecycleActionNone,
		},
		{
			name:                         "Stopped + lifecycle.phase=stopped -> None (still hibernated)",
			currentStatus:                acidv1.ClusterStatusStopped,
			newLifecyclePhase:            "stopped",
			newNumberOfInstances:         0,
			newPreviousNumberOfInstances: 3,
			want:                         LifecycleActionNone,
		},
		{
			name:                         "Stopped + lifecycle cleared + has previous instances + numInst=0 -> WakeUp",
			currentStatus:                acidv1.ClusterStatusStopped,
			newLifecyclePhase:            "",
			newNumberOfInstances:         0,
			newPreviousNumberOfInstances: 3,
			want:                         LifecycleActionWakeUp,
		},
		{
			name:                         "Running + lifecycle cleared + previous instances + numInst=0 -> WakeUp",
			currentStatus:                acidv1.ClusterStatusRunning,
			newLifecyclePhase:            "",
			newNumberOfInstances:         0,
			newPreviousNumberOfInstances: 3,
			want:                         LifecycleActionWakeUp,
		},
		{
			name:                         "Running + lifecycle cleared + previous instances + numInst>0 -> None",
			currentStatus:                acidv1.ClusterStatusRunning,
			newLifecyclePhase:            "",
			newNumberOfInstances:         3,
			newPreviousNumberOfInstances: 3,
			want:                         LifecycleActionNone,
		},
		{
			name:                         "Running + lifecycle cleared + no previous instances -> None",
			currentStatus:                acidv1.ClusterStatusRunning,
			newLifecyclePhase:            "",
			newNumberOfInstances:         3,
			newPreviousNumberOfInstances: 0,
			want:                         LifecycleActionNone,
		},
		{
			name:                         "Stopped + lifecycle cleared + no previous instances -> WakeUp (operator restart catch-up)",
			currentStatus:                acidv1.ClusterStatusStopped,
			newLifecyclePhase:            "",
			newNumberOfInstances:         0,
			newPreviousNumberOfInstances: 0,
			want:                         LifecycleActionWakeUp,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var lifecycle *acidv1.LifecycleSpec
			if tt.newLifecyclePhase != "" {
				lifecycle = &acidv1.LifecycleSpec{Phase: tt.newLifecyclePhase}
			}

			status := acidv1.PostgresStatus{PostgresClusterStatus: tt.currentStatus}

			got := detectLifecycleTransition(
				&status,
				lifecycle,
				tt.newNumberOfInstances,
				tt.newPreviousNumberOfInstances,
			)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestInitiateHibernate(t *testing.T) {
	client, _, _ := newFakeK8sClientForLifecycle()
	c := newLifecycleCluster(client, acidv1.ClusterStatusRunning, 3, "", 0, nil)

	newSpec := &acidv1.Postgresql{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec: acidv1.PostgresSpec{
			NumberOfInstances: 3,
		},
		Status: acidv1.PostgresStatus{
			PostgresClusterStatus: acidv1.ClusterStatusRunning,
		},
	}

	c.initiateHibernate(newSpec)

	assert.Equal(t, int32(0), newSpec.Spec.NumberOfInstances, "numberOfInstances should be set to 0")
	assert.Equal(t, int32(3), newSpec.Status.PreviousNumberOfInstances, "previousNumberOfInstances should be stored")
	assert.Equal(t, acidv1.ClusterStatusStopping, newSpec.Status.PostgresClusterStatus, "status should be Stopping")
}

func TestInitiateWakeUp(t *testing.T) {
	t.Run("restores numberOfInstances and clears Previous fields", func(t *testing.T) {
		client, _, _ := newFakeK8sClientForLifecycle()
		c := newLifecycleCluster(client, acidv1.ClusterStatusStopped, 0, "", 0, nil)

		newSpec := &acidv1.Postgresql{
			ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
			Spec: acidv1.PostgresSpec{
				NumberOfInstances: 0,
			},
			Status: acidv1.PostgresStatus{
				PostgresClusterStatus:      acidv1.ClusterStatusStopped,
				PreviousNumberOfInstances:  3,
				PreviousPoolerInstances:    map[string]int32{"master": 2, "replica": 0},
			},
		}

		c.initiateWakeUp(newSpec)

		assert.Equal(t, int32(3), newSpec.Spec.NumberOfInstances, "numberOfInstances should be restored")
		assert.Equal(t, acidv1.ClusterStatusUpdating, newSpec.Status.PostgresClusterStatus, "status should be Updating")
		assert.Equal(t, int32(0), newSpec.Status.PreviousNumberOfInstances, "previousNumberOfInstances should be cleared")
		assert.Nil(t, newSpec.Status.PreviousPoolerInstances, "previousPoolerInstances should be cleared")
	})

	t.Run("previousNumberOfInstances=0 still transitions to Updating", func(t *testing.T) {
		client, _, _ := newFakeK8sClientForLifecycle()
		c := newLifecycleCluster(client, acidv1.ClusterStatusStopped, 0, "", 0, nil)

		newSpec := &acidv1.Postgresql{
			ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
			Spec: acidv1.PostgresSpec{
				NumberOfInstances: 0,
			},
			Status: acidv1.PostgresStatus{
				PostgresClusterStatus:     acidv1.ClusterStatusStopped,
				PreviousPoolerInstances:   map[string]int32{"master": 1},
			},
		}

		c.initiateWakeUp(newSpec)

		assert.Equal(t, int32(0), newSpec.Spec.NumberOfInstances, "numberOfInstances stays 0 when previous is 0")
		assert.Equal(t, acidv1.ClusterStatusUpdating, newSpec.Status.PostgresClusterStatus, "status should still be Updating")
		assert.Equal(t, int32(0), newSpec.Status.PreviousNumberOfInstances, "previousNumberOfInstances stays 0")
		assert.Nil(t, newSpec.Status.PreviousPoolerInstances, "previousPoolerInstances should be cleared")
	})
}

func TestPrepareLifecycleTransition_Hibernate(t *testing.T) {
	client, _, _ := newFakeK8sClientForLifecycle()
	c := newLifecycleCluster(client, acidv1.ClusterStatusRunning, 3, "", 0, nil)

	newSpec := c.Postgresql.DeepCopy()
	newSpec.Spec.Lifecycle = &acidv1.LifecycleSpec{Phase: "stopped"}

	oldSpec := acidv1.Postgresql{
		Status: acidv1.PostgresStatus{PostgresClusterStatus: acidv1.ClusterStatusRunning},
	}

	proceed, err := c.prepareLifecycleTransition(&newSpec, oldSpec)

	assert.NoError(t, err)
	assert.True(t, proceed, "hibernate should proceed with sync")
	assert.Equal(t, int32(0), newSpec.Spec.NumberOfInstances, "numberOfInstances should be set to 0")
	assert.Equal(t, int32(3), newSpec.Status.PreviousNumberOfInstances, "previousNumberOfInstances should be stored")
	assert.Equal(t, acidv1.ClusterStatusStopping, newSpec.Status.PostgresClusterStatus, "status should be Stopping")

	persisted, err := client.Postgresqls("default").Get(context.TODO(), "test-cluster", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, acidv1.ClusterStatusStopping, persisted.Status.PostgresClusterStatus, "persisted status should be Stopping")
	assert.Equal(t, int32(3), persisted.Status.PreviousNumberOfInstances, "persisted previousNumberOfInstances should be 3")
	assert.Equal(t, int32(0), persisted.Spec.NumberOfInstances, "persisted numberOfInstances should be 0")
}

func TestPrepareLifecycleTransition_WakeUp(t *testing.T) {
	client, _, _ := newFakeK8sClientForLifecycle()
	c := newLifecycleCluster(
		client,
		acidv1.ClusterStatusStopped,
		0,
		"",
		3,
		map[string]int32{"master": 2, "replica": 0},
	)

	newSpec := c.Postgresql.DeepCopy()

	oldSpec := acidv1.Postgresql{
		Status: acidv1.PostgresStatus{PostgresClusterStatus: acidv1.ClusterStatusStopped},
	}

	proceed, err := c.prepareLifecycleTransition(&newSpec, oldSpec)

	assert.NoError(t, err)
	assert.True(t, proceed, "wake-up should proceed with sync")
	assert.Equal(t, int32(3), newSpec.Spec.NumberOfInstances, "numberOfInstances should be restored")
	assert.Equal(t, acidv1.ClusterStatusUpdating, newSpec.Status.PostgresClusterStatus, "status should be Updating")
	assert.Equal(t, int32(0), newSpec.Status.PreviousNumberOfInstances, "previousNumberOfInstances should be cleared after persistence")
	assert.Nil(t, newSpec.Status.PreviousPoolerInstances, "previousPoolerInstances should be cleared after persistence")

	persisted, err := client.Postgresqls("default").Get(context.TODO(), "test-cluster", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, acidv1.ClusterStatusUpdating, persisted.Status.PostgresClusterStatus, "persisted status should be Updating")
	assert.Equal(t, int32(3), persisted.Spec.NumberOfInstances, "persisted numberOfInstances should be 3")
	assert.Equal(t, int32(0), persisted.Status.PreviousNumberOfInstances, "persisted previousNumberOfInstances should be 0")
	assert.Nil(t, persisted.Status.PreviousPoolerInstances, "persisted previousPoolerInstances should be nil")
}

func TestPrepareLifecycleTransition_StoppedNoTransition(t *testing.T) {
	client, _, acidClientSet := newFakeK8sClientForLifecycle()
	c := newLifecycleCluster(
		client,
		acidv1.ClusterStatusStopped,
		0,
		"stopped",
		3,
		nil,
	)

	updateCalled := false
	acidClientSet.PrependReactor("update", "postgresqls", func(action k8stesting.Action) (bool, runtime.Object, error) {
		updateCalled = true
		return false, nil, nil
	})

	newSpec := c.Postgresql.DeepCopy()

	oldSpec := acidv1.Postgresql{
		Status: acidv1.PostgresStatus{PostgresClusterStatus: acidv1.ClusterStatusStopped},
	}

	proceed, err := c.prepareLifecycleTransition(&newSpec, oldSpec)

	assert.NoError(t, err)
	assert.False(t, proceed, "stopped cluster with lifecycle=stopped should skip sync")
	assert.False(t, updateCalled, "no K8s API writes should happen for stopped-no-transition")
	assert.Equal(t, int32(0), newSpec.Spec.NumberOfInstances, "numberOfInstances should be unchanged")
	assert.Equal(t, acidv1.ClusterStatusStopped, newSpec.Status.PostgresClusterStatus, "status should remain Stopped")
}

func TestPrepareLifecycleTransition_NoTransitionRunning(t *testing.T) {
	client, _, acidClientSet := newFakeK8sClientForLifecycle()
	c := newLifecycleCluster(client, acidv1.ClusterStatusRunning, 3, "", 0, nil)

	updateCalled := false
	acidClientSet.PrependReactor("update", "postgresqls", func(action k8stesting.Action) (bool, runtime.Object, error) {
		updateCalled = true
		return false, nil, nil
	})

	newSpec := c.Postgresql.DeepCopy()

	oldSpec := acidv1.Postgresql{
		Status: acidv1.PostgresStatus{PostgresClusterStatus: acidv1.ClusterStatusRunning},
	}

	proceed, err := c.prepareLifecycleTransition(&newSpec, oldSpec)

	assert.NoError(t, err)
	assert.True(t, proceed, "no-transition running cluster should proceed with sync")
	assert.False(t, updateCalled, "no K8s API writes should happen for no-transition")
	assert.Equal(t, int32(3), newSpec.Spec.NumberOfInstances, "numberOfInstances should be unchanged")
	assert.Equal(t, acidv1.ClusterStatusRunning, newSpec.Status.PostgresClusterStatus, "status should remain Running")
}

func TestPrepareLifecycleTransition_UpdateSpecFails(t *testing.T) {
	client, _, acidClientSet := newFakeK8sClientForLifecycle()
	c := newLifecycleCluster(client, acidv1.ClusterStatusRunning, 3, "", 0, nil)

	acidClientSet.PrependReactor("update", "postgresqls", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("api server unavailable")
	})

	newSpec := c.Postgresql.DeepCopy()
	newSpec.Spec.Lifecycle = &acidv1.LifecycleSpec{Phase: "stopped"}

	oldSpec := acidv1.Postgresql{
		Status: acidv1.PostgresStatus{PostgresClusterStatus: acidv1.ClusterStatusRunning},
	}

	proceed, err := c.prepareLifecycleTransition(&newSpec, oldSpec)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not update spec for lifecycle action")
	assert.False(t, proceed, "should not proceed when spec write fails")
}

func TestPersistStoppingCompletedTransition(t *testing.T) {
	client, _, _ := newFakeK8sClientForLifecycle()
	c := newLifecycleCluster(client, acidv1.ClusterStatusStopping, 0, "stopped", 3, nil)

	newSpec := c.Postgresql.DeepCopy()
	newSpec.Status.PostgresClusterStatus = acidv1.ClusterStatusStopped

	handled, err := c.persistStoppingCompletedTransition(newSpec)

	assert.NoError(t, err)
	assert.True(t, handled, "should report handled=true on success")

	persisted, err := client.Postgresqls("default").Get(context.TODO(), "test-cluster", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, acidv1.ClusterStatusStopped, persisted.Status.PostgresClusterStatus, "persisted status should be Stopped")
	assert.Equal(t, acidv1.ClusterStatusStopped, c.Postgresql.Status.PostgresClusterStatus, "cache should reflect Stopped")
}

func TestGetPoolerReplicas(t *testing.T) {
	tests := []struct {
		name       string
		poolerObjs map[PostgresRole]*ConnectionPoolerObjects
		role       PostgresRole
		want       int32
	}{
		{
			name:       "nil ConnectionPooler map",
			poolerObjs: nil,
			role:       Master,
			want:       0,
		},
		{
			name:       "ConnectionPooler for role is nil",
			poolerObjs: map[PostgresRole]*ConnectionPoolerObjects{Master: nil},
			role:       Master,
			want:       0,
		},
		{
			name:       "Deployment is nil",
			poolerObjs: map[PostgresRole]*ConnectionPoolerObjects{Master: {Deployment: nil}},
			role:       Master,
			want:       0,
		},
		{
			name: "Replicas is nil",
			poolerObjs: map[PostgresRole]*ConnectionPoolerObjects{
				Master: {Deployment: &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: nil}}},
			},
			role: Master,
			want: 0,
		},
		{
			name: "Master with 2 replicas",
			poolerObjs: map[PostgresRole]*ConnectionPoolerObjects{
				Master: newTestPoolerObjects(Master, 2),
			},
			role: Master,
			want: 2,
		},
		{
			name: "Master with 0 replicas",
			poolerObjs: map[PostgresRole]*ConnectionPoolerObjects{
				Master: newTestPoolerObjects(Master, 0),
			},
			role: Master,
			want: 0,
		},
		{
			name: "Replica with 3 replicas",
			poolerObjs: map[PostgresRole]*ConnectionPoolerObjects{
				Replica: newTestPoolerObjects(Replica, 3),
			},
			role: Replica,
			want: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Cluster{
				ConnectionPooler: tt.poolerObjs,
			}
			got := c.getPoolerReplicas(tt.role)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPatchPoolerReplicas(t *testing.T) {
	tests := []struct {
		name        string
		replicas    int32
		setupClient func(clientSet *fake.Clientset)
		wantErr     bool
		errContains string
	}{
		{
			name:     "deployment exists, patch succeeds",
			replicas: 0,
			setupClient: func(clientSet *fake.Clientset) {
				_, _ = clientSet.AppsV1().Deployments("default").Create(context.TODO(), &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{Name: "test-cluster-pooler"},
					Spec:       appsv1.DeploymentSpec{Replicas: int32Ptr(2)},
				}, metav1.CreateOptions{})
			},
			wantErr: false,
		},
		{
			name:        "deployment not found - returns nil",
			replicas:    2,
			setupClient: func(clientSet *fake.Clientset) {},
			wantErr:     false,
		},
		{
			name:     "patch returns error",
			replicas: 2,
			setupClient: func(clientSet *fake.Clientset) {
				_, _ = clientSet.AppsV1().Deployments("default").Create(context.TODO(), &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{Name: "test-cluster-pooler"},
					Spec:       appsv1.DeploymentSpec{Replicas: int32Ptr(2)},
				}, metav1.CreateOptions{})
				clientSet.PrependReactor("patch", "deployments", func(action k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, fmt.Errorf("network error")
				})
			},
			wantErr:     true,
			errContains: "could not patch pooler deployment",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientSet := fake.NewSimpleClientset()
			if tt.setupClient != nil {
				tt.setupClient(clientSet)
			}

			kubeClient := &k8sutil.KubernetesClient{
				DeploymentsGetter: clientSet.AppsV1(),
			}

			c := &Cluster{
				KubeClient: *kubeClient,
				Postgresql: acidv1.Postgresql{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cluster",
						Namespace: "default",
					},
				},
			}

			err := c.patchPoolerReplicas(Master, tt.replicas)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestScalePoolerDown(t *testing.T) {
	tests := []struct {
		name       string
		poolerObjs map[PostgresRole]*ConnectionPoolerObjects
		wantStored map[string]int32
	}{
		{
			name:       "nil ConnectionPooler - no-op",
			poolerObjs: nil,
			wantStored: nil,
		},
		{
			name:       "Master at 2 replicas",
			poolerObjs: map[PostgresRole]*ConnectionPoolerObjects{Master: newTestPoolerObjects(Master, 2)},
			wantStored: map[string]int32{"master": 2},
		},
		{
			name:       "Master already at 0 replicas",
			poolerObjs: map[PostgresRole]*ConnectionPoolerObjects{Master: newTestPoolerObjects(Master, 0)},
			wantStored: map[string]int32{"master": 0},
		},
		{
			name: "Both Master and Replica",
			poolerObjs: map[PostgresRole]*ConnectionPoolerObjects{
				Master:  newTestPoolerObjects(Master, 2),
				Replica: newTestPoolerObjects(Replica, 1),
			},
			wantStored: map[string]int32{"master": 2, "replica": 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientSet := fake.NewSimpleClientset()
			kubeClient := &k8sutil.KubernetesClient{
				DeploymentsGetter: clientSet.AppsV1(),
			}

			c := &Cluster{
				KubeClient:       *kubeClient,
				ConnectionPooler: tt.poolerObjs,
				logger:           lifecycleLogger,
				Postgresql: acidv1.Postgresql{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cluster",
						Namespace: "default",
					},
				},
			}

			newSpec := &acidv1.Postgresql{}
			err := c.scalePoolerDown(newSpec)

			assert.NoError(t, err)
			assert.Equal(t, tt.wantStored, newSpec.Status.PreviousPoolerInstances)
		})
	}
}

func TestScalePoolerUp(t *testing.T) {
	tests := []struct {
		name     string
		prevInst map[string]int32
		wantErr  bool
	}{
		{
			name:     "nil PreviousPoolerInstances - no-op",
			prevInst: nil,
			wantErr:  false,
		},
		{
			name:     "Restore master to 2",
			prevInst: map[string]int32{"master": 2},
			wantErr:  false,
		},
		{
			name:     "Restore both roles",
			prevInst: map[string]int32{"master": 2, "replica": 1},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientSet := fake.NewSimpleClientset()
			kubeClient := &k8sutil.KubernetesClient{
				DeploymentsGetter: clientSet.AppsV1(),
			}

			c := &Cluster{
				KubeClient: *kubeClient,
				logger:     lifecycleLogger,
				Postgresql: acidv1.Postgresql{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cluster",
						Namespace: "default",
					},
				},
			}

			newSpec := &acidv1.Postgresql{
				Status: acidv1.PostgresStatus{
					PreviousPoolerInstances: tt.prevInst,
				},
			}
			err := c.scalePoolerUp(newSpec)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestSuspendLogicalBackupJob(t *testing.T) {
	tests := []struct {
		name       string
		jobExists  bool
		patchFails bool
		wantErr    bool
	}{
		{
			name:      "job exists, suspend succeeds",
			jobExists: true,
			wantErr:   false,
		},
		{
			name:      "job does not exist - no-op",
			jobExists: false,
			wantErr:   false,
		},
		{
			name:       "job exists but patch fails",
			jobExists:  true,
			patchFails: true,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientSet := fake.NewSimpleClientset()
			jobName := "logical-backup-test-cluster"

			if tt.jobExists {
				_, _ = clientSet.BatchV1().CronJobs("default").Create(context.TODO(), &batchv1.CronJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      jobName,
						Namespace: "default",
					},
					Spec: batchv1.CronJobSpec{
						Schedule: "30 00 * * *",
					},
				}, metav1.CreateOptions{})
			}

			if tt.patchFails {
				clientSet.PrependReactor("patch", "cronjobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, fmt.Errorf("network error")
				})
			}

			kubeClient := &k8sutil.KubernetesClient{
				CronJobsGetter: clientSet.BatchV1(),
			}

			var job *batchv1.CronJob
			if tt.jobExists {
				job, _ = kubeClient.CronJobs("default").Get(context.TODO(), jobName, metav1.GetOptions{})
			}

			c := New(
				Config{
					OpConfig: config.Config{
						LogicalBackup: config.LogicalBackup{
							LogicalBackupJobPrefix: "logical-backup-",
						},
					},
				},
				*kubeClient,
				acidv1.Postgresql{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cluster",
						Namespace: "default",
					},
				},
				lifecycleLogger,
				lifecycleEventRecorder,
			)
			c.LogicalBackupJob = job

			err := c.suspendLogicalBackupJob()

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.jobExists && !tt.patchFails {
					updatedJob, _ := kubeClient.CronJobs("default").Get(context.TODO(), jobName, metav1.GetOptions{})
					if updatedJob != nil {
						assert.True(t, *updatedJob.Spec.Suspend, "job should be suspended")
					}
				}
			}
		})
	}
}

func TestUnsuspendLogicalBackupJob(t *testing.T) {
	tests := []struct {
		name       string
		jobExists  bool
		patchFails bool
		wantErr    bool
	}{
		{
			name:      "job exists, unsuspend succeeds",
			jobExists: true,
			wantErr:   false,
		},
		{
			name:      "job does not exist - no-op",
			jobExists: false,
			wantErr:   false,
		},
		{
			name:       "job exists but patch fails",
			jobExists:  true,
			patchFails: true,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientSet := fake.NewSimpleClientset()
			jobName := "logical-backup-test-cluster"

			if tt.jobExists {
				suspendTrue := true
				_, _ = clientSet.BatchV1().CronJobs("default").Create(context.TODO(), &batchv1.CronJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      jobName,
						Namespace: "default",
					},
					Spec: batchv1.CronJobSpec{
						Schedule: "30 00 * * *",
						Suspend:  &suspendTrue,
					},
				}, metav1.CreateOptions{})
			}

			if tt.patchFails {
				clientSet.PrependReactor("patch", "cronjobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, fmt.Errorf("network error")
				})
			}

			kubeClient := &k8sutil.KubernetesClient{
				CronJobsGetter: clientSet.BatchV1(),
			}

			var job *batchv1.CronJob
			if tt.jobExists {
				job, _ = kubeClient.CronJobs("default").Get(context.TODO(), jobName, metav1.GetOptions{})
			}

			c := New(
				Config{
					OpConfig: config.Config{
						LogicalBackup: config.LogicalBackup{
							LogicalBackupJobPrefix: "logical-backup-",
						},
					},
				},
				*kubeClient,
				acidv1.Postgresql{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cluster",
						Namespace: "default",
					},
				},
				lifecycleLogger,
				lifecycleEventRecorder,
			)
			c.LogicalBackupJob = job

			err := c.unsuspendLogicalBackupJob()

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.jobExists && !tt.patchFails {
					updatedJob, _ := kubeClient.CronJobs("default").Get(context.TODO(), jobName, metav1.GetOptions{})
					if updatedJob != nil {
						assert.False(t, *updatedJob.Spec.Suspend, "job should be unsuspended")
					}
				}
			}
		})
	}
}

// blockLifecycleUpdate lives in cluster.go but is exercised here because the
// lifecycle subsystem owns its semantics (Stopped/Stopping states).
func TestBlockLifecycleUpdate(t *testing.T) {
	tests := []struct {
		name           string
		currentStatus  string
		lifecyclePhase string
		wantBlocked    bool
		wantErr        bool
		errContains    string
	}{
		{
			name:          "Running cluster, allows update",
			currentStatus: acidv1.ClusterStatusRunning,
			wantBlocked:   false,
			wantErr:       false,
		},
		{
			name:          "Stopping state, blocks update",
			currentStatus: acidv1.ClusterStatusStopping,
			wantBlocked:   true,
			wantErr:       true,
			errContains:   "cannot update cluster while it is stopping",
		},
		{
			name:           "Stopped with lifecycle.phase=stopped, blocks update",
			currentStatus:  acidv1.ClusterStatusStopped,
			lifecyclePhase: "stopped",
			wantBlocked:    true,
			wantErr:        true,
			errContains:    "cannot update cluster while stopped",
		},
		{
			name:           "Stopped without lifecycle.phase, allows update (wake-up)",
			currentStatus:  acidv1.ClusterStatusStopped,
			lifecyclePhase: "",
			wantBlocked:    false,
			wantErr:        false,
		},
		{
			name:          "UpdateFailed state, allows update",
			currentStatus: acidv1.ClusterStatusUpdateFailed,
			wantBlocked:   false,
			wantErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, _, _ := newFakeK8sClientForLifecycle()
			c := newLifecycleCluster(client, tt.currentStatus, 3, tt.lifecyclePhase, 0, nil)

			newSpec := c.Postgresql.DeepCopy()
			if tt.lifecyclePhase != "" && tt.currentStatus != acidv1.ClusterStatusStopped {
				newSpec.Spec.Lifecycle = &acidv1.LifecycleSpec{Phase: tt.lifecyclePhase}
			}

			blocked, err := c.blockLifecycleUpdate(newSpec)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.wantBlocked, blocked)
		})
	}
}

func TestLifecycleUpdateBlocksDuringStopping(t *testing.T) {
	client, _, _ := newFakeK8sClientForLifecycle()
	c := newLifecycleCluster(client, acidv1.ClusterStatusStopping, 0, "stopped", 3, nil)

	newSpec := c.Postgresql.DeepCopy()
	blocked, err := c.blockLifecycleUpdate(newSpec)

	assert.True(t, blocked)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot update cluster while it is stopping")
}

func TestLifecycleUpdateBlocksWhenStoppedWithPhase(t *testing.T) {
	client, _, _ := newFakeK8sClientForLifecycle()
	c := newLifecycleCluster(client, acidv1.ClusterStatusStopped, 0, "stopped", 3, nil)

	newSpec := c.Postgresql.DeepCopy()
	blocked, err := c.blockLifecycleUpdate(newSpec)

	assert.True(t, blocked)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot update cluster while stopped")
}

func TestLifecycleUpdateAllowsWakeUp(t *testing.T) {
	client, _, _ := newFakeK8sClientForLifecycle()
	c := newLifecycleCluster(client, acidv1.ClusterStatusStopped, 0, "", 3, nil)

	newSpec := c.Postgresql.DeepCopy()
	blocked, err := c.blockLifecycleUpdate(newSpec)

	assert.False(t, blocked)
	assert.NoError(t, err)
}
