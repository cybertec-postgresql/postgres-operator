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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
)

var lifecycleLogger = logrus.New().WithField("test", "lifecycle")
var lifecycleEventRecorder = record.NewFakeRecorder(10)

func int32Ptr(i int32) *int32 { return &i }

func newTestLifecycleCluster(status string, numberOfInstances int32, lifecyclePhase string) *Cluster {
	pg := acidv1.Postgresql{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: acidv1.PostgresSpec{
			TeamID:           "test-team",
			NumberOfInstances: numberOfInstances,
			Volume:           acidv1.Volume{Size: "1Gi"},
		},
		Status: acidv1.PostgresStatus{
			PostgresClusterStatus: status,
		},
	}

	if lifecyclePhase != "" {
		pg.Spec.Lifecycle = &acidv1.LifecycleSpec{
			Phase: lifecyclePhase,
		}
	}

	return &Cluster{
		Config: Config{
			OpConfig: config.Config{
				PodManagementPolicy: "ordered_ready",
			},
		},
		Postgresql: pg,
		logger:     lifecycleLogger,
		eventRecorder: lifecycleEventRecorder,
	}
}

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
		DeploymentsGetter:  clientSet.AppsV1(),
		PostgresqlsGetter:  acidClientSet.AcidV1(),
		StatefulSetsGetter: clientSet.AppsV1(),
		ServicesGetter:     clientSet.CoreV1(),
		SecretsGetter:      clientSet.CoreV1(),
		ConfigMapsGetter:    clientSet.CoreV1(),
		PodsGetter:         clientSet.CoreV1(),
		EndpointsGetter:    clientSet.CoreV1(),
	}

	return client, clientSet, acidClientSet
}

func createTestPostgresqlInClient(client *k8sutil.KubernetesClient, name, namespace string, spec *acidv1.PostgresSpec, status *acidv1.PostgresStatus) *acidv1.Postgresql {
	pg := &acidv1.Postgresql{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
	if spec != nil {
		pg.Spec = *spec
	}
	if status != nil {
		pg.Status = *status
	}

	created, err := client.Postgresqls(namespace).Create(context.TODO(), pg, metav1.CreateOptions{})
	if err == nil {
		return created
	}
	return pg
}

func updatePostgresqlInClient(client *k8sutil.KubernetesClient, pg *acidv1.Postgresql) (*acidv1.Postgresql, error) {
	return client.Postgresqls(pg.Namespace).Update(context.TODO(), pg, metav1.UpdateOptions{})
}

func updatePostgresqlStatusInClient(client *k8sutil.KubernetesClient, pg *acidv1.Postgresql) (*acidv1.Postgresql, error) {
	return client.Postgresqls(pg.Namespace).UpdateStatus(context.TODO(), pg, metav1.UpdateOptions{})
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
			name:       "deployment exists, patch succeeds",
			replicas:    0,
			setupClient: func(clientSet *fake.Clientset) {
				// Pre-create the deployment in the fake clientset
				clientSet.AppsV1().Deployments("default").Create(context.TODO(), &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{Name: "test-cluster-pooler"},
					Spec:      appsv1.DeploymentSpec{Replicas: int32Ptr(2)},
				}, metav1.CreateOptions{})
			},
			wantErr: false,
		},
		{
			name:       "deployment not found",
			replicas:    2,
			setupClient: func(clientSet *fake.Clientset) {
				// Don't create anything - will trigger NotFound
			},
			wantErr: false, // NotFound is handled gracefully
		},
		{
			name:       "patch returns error",
			replicas:    2,
			setupClient: func(clientSet *fake.Clientset) {
				// Pre-create deployment but make patch fail via reactor
				clientSet.AppsV1().Deployments("default").Create(context.TODO(), &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{Name: "test-cluster-pooler"},
					Spec:      appsv1.DeploymentSpec{Replicas: int32Ptr(2)},
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
			currentStatus: "Running",
			wantBlocked:   false,
			wantErr:       false,
		},
		{
			name:          "Stopping state, blocks update",
			currentStatus: "Stopping",
			wantBlocked:   true,
			wantErr:       true,
			errContains:   "cannot update cluster while it is stopping",
		},
		{
			name:           "Stopped with lifecycle phase, blocks update",
			currentStatus:  "Stopped",
			lifecyclePhase: "stopped",
			wantBlocked:    true,
			wantErr:        true,
			errContains:   "cannot update cluster while stopped",
		},
		{
			name:           "Stopped without lifecycle phase, allows update (wake-up)",
			currentStatus:  "Stopped",
			lifecyclePhase: "",
			wantBlocked:    false,
			wantErr:        false,
		},
		{
			name:           "Stopped with nil lifecycle, allows update (wake-up)",
			currentStatus:  "Stopped",
			lifecyclePhase: "",
			wantBlocked:    false,
			wantErr:        false,
		},
		{
			name:           "Stopped with empty lifecycle phase, allows update (wake-up)",
			currentStatus:  "Stopped",
			lifecyclePhase: "",
			wantBlocked:    false,
			wantErr:        false,
		},
		{
			name:          "UpdateFailed state, allows update",
			currentStatus: "UpdateFailed",
			wantBlocked:   false,
			wantErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestLifecycleCluster(tt.currentStatus, 3, tt.lifecyclePhase)

			newSpec := c.DeepCopy()
			if tt.lifecyclePhase != "" && tt.currentStatus != "Stopped" {
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

func TestManageHibernateState(t *testing.T) {
	tests := []struct {
		name                   string
		oldSpecStatus          string
		newSpecStatus          string
		newSpecLifecyclePhase  string
		numberOfInstances      int32
		previousNumberOfInst   int32
		statefulsetReplicas    *int32
		wantContinue           bool
		wantNumberOfInstances  *int32
		wantStatus             string
	}{
		{
			name:                  "Running to Stopping - initiates hibernate",
			oldSpecStatus:         "Running",
			newSpecStatus:         "Running",
			newSpecLifecyclePhase: "stopped",
			numberOfInstances:     3,
			statefulsetReplicas:   int32Ptr(3),
			wantContinue:          true,
			wantNumberOfInstances: int32Ptr(0),
			wantStatus:            "Stopping",
		},
		{
			name:                 "Stopping to Stopped - when replicas reach 0",
			oldSpecStatus:        "Stopping",
			newSpecStatus:        "Stopping",
			numberOfInstances:     0,
			statefulsetReplicas:  int32Ptr(0),
			wantContinue:         true,
			wantNumberOfInstances: int32Ptr(0),
			wantStatus:           "Stopped",
		},
		{
			name:                 "Stopping - replicas not yet 0",
			oldSpecStatus:        "Stopping",
			newSpecStatus:        "Stopping",
			numberOfInstances:     0,
			statefulsetReplicas:  int32Ptr(2), // Still terminating
			wantContinue:         true,
			wantNumberOfInstances: nil, // Should NOT change
			wantStatus:           "Stopping", // Should NOT change
		},
		{
			name:                  "Stopped to wake-up - restores numberOfInstances",
			oldSpecStatus:         "Stopped",
			newSpecStatus:         "Stopped",
			newSpecLifecyclePhase: "", // Cleared
			numberOfInstances:      0,
			previousNumberOfInst:   3,
			statefulsetReplicas:   int32Ptr(0),
			wantContinue:          true,
			wantNumberOfInstances: int32Ptr(3),
			wantStatus:            "Updating",
		},
		{
			name:                 "Stopped but lifecycle still 'stopped' - skip sync",
			oldSpecStatus:        "Stopped",
			newSpecStatus:        "Stopped",
			newSpecLifecyclePhase: "stopped",
			numberOfInstances:     0,
			statefulsetReplicas:  int32Ptr(0),
			wantContinue:         false, // Should skip sync
			wantNumberOfInstances: nil,
			wantStatus:           "Stopped",
		},
		{
			name:                  "Running without lifecycle - continue normal",
			oldSpecStatus:         "Running",
			newSpecStatus:         "Running",
			newSpecLifecyclePhase: "",
			numberOfInstances:     3,
			statefulsetReplicas:  int32Ptr(3),
			wantContinue:         true,
			wantNumberOfInstances: int32Ptr(3), // Unchanged
			wantStatus:           "Running",      // Unchanged
		},
		{
			name:                  "Running to Updating - normal update",
			oldSpecStatus:         "Running",
			newSpecStatus:         "Updating",
			newSpecLifecyclePhase: "",
			numberOfInstances:     3,
			statefulsetReplicas:  int32Ptr(3),
			wantContinue:         true,
			wantNumberOfInstances: int32Ptr(3),
			wantStatus:           "Updating",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Cluster{
				logger: lifecycleLogger,
				Postgresql: acidv1.Postgresql{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cluster",
						Namespace: "default",
					},
				},
			}

			if tt.statefulsetReplicas != nil {
				c.Statefulset = &appsv1.StatefulSet{
					Spec: appsv1.StatefulSetSpec{
						Replicas: tt.statefulsetReplicas,
					},
				}
			}

			oldSpec := acidv1.Postgresql{
				Status: acidv1.PostgresStatus{
					PostgresClusterStatus: tt.oldSpecStatus,
				},
			}

			newSpec := &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					NumberOfInstances: tt.numberOfInstances,
				},
				Status: acidv1.PostgresStatus{
					PostgresClusterStatus:      tt.newSpecStatus,
					PreviousNumberOfInstances: tt.previousNumberOfInst,
				},
			}

			if tt.newSpecLifecyclePhase != "" {
				newSpec.Spec.Lifecycle = &acidv1.LifecycleSpec{
					Phase: tt.newSpecLifecyclePhase,
				}
			}

			gotContinue := c.manageHibernateState(oldSpec, newSpec)

			assert.Equal(t, tt.wantContinue, gotContinue)

			if tt.wantNumberOfInstances != nil {
				assert.Equal(t, *tt.wantNumberOfInstances, newSpec.Spec.NumberOfInstances)
			}
			assert.Equal(t, tt.wantStatus, newSpec.Status.PostgresClusterStatus)
		})
	}
}

func TestHandleHibernateAndWakeUp_Hibernate(t *testing.T) {
	tests := []struct {
		name                string
		currentStatus       string
		numberOfInstances   int32
		lifecyclePhase      string
		poolerObjs         map[PostgresRole]*ConnectionPoolerObjects
		k8sUpdateSucceeds   bool
		k8sStatusSucceeds   bool
		poolerPatchSucceeds bool
		wantHandled         bool
		wantErr             bool
		errContains         string
	}{
		{
			name:               "Running + lifecycle.phase=stopped - initiates hibernate",
			currentStatus:      "Running",
			numberOfInstances:  3,
			lifecyclePhase:     "stopped",
			k8sUpdateSucceeds: true,
			k8sStatusSucceeds:  true,
			poolerPatchSucceeds: true,
			wantHandled: true,
			wantErr:    false,
		},
		{
			name:               "Running + lifecycle.phase=stopped - K8s update fails",
			currentStatus:      "Running",
			numberOfInstances:  3,
			lifecyclePhase:     "stopped",
			k8sUpdateSucceeds: false,
			wantHandled: false,
			wantErr:     true,
			errContains: "could not update spec during hibernate",
		},
		{
			name:               "Running + lifecycle.phase=stopped - K8s status update fails",
			currentStatus:      "Running",
			numberOfInstances:  3,
			lifecyclePhase:     "stopped",
			k8sUpdateSucceeds: true,
			k8sStatusSucceeds: false,
			wantHandled: false,
			wantErr:     true,
			errContains: "could not update status during hibernate",
		},
		{
			name:               "Running + no lifecycle phase - no transition",
			currentStatus:      "Running",
			numberOfInstances:  3,
			lifecyclePhase:      "",
			wantHandled: false,
			wantErr:    false,
		},
		{
			name:               "Stopped + lifecycle cleared - no-op (Update handles this)",
			currentStatus:      "Stopped",
			numberOfInstances:  0,
			lifecyclePhase:      "",
			wantHandled: false, // handleHibernateAndWakeUp doesn't handle wake-up, that's done via Update flow
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kubeClient, _, acidClientSet := newFakeK8sClientForLifecycle()

			if tt.k8sUpdateSucceeds {
				acidClientSet.PrependReactor("update", "postgresqls", func(action k8stesting.Action) (bool, runtime.Object, error) {
					updateAction := action.(k8stesting.UpdateAction)
					pg := updateAction.GetObject().(*acidv1.Postgresql)
					return true, pg, nil
				})
			}

			if !tt.k8sStatusSucceeds {
				acidClientSet.PrependReactor("update", "postgresqls", func(action k8stesting.Action) (bool, runtime.Object, error) {
					if action.GetSubresource() == "status" {
						return true, nil, fmt.Errorf("status update failed")
					}
					return false, nil, nil
				})
			}

			c := &Cluster{
				Config: Config{
					OpConfig: config.Config{
						PodManagementPolicy: "ordered_ready",
					},
				},
				Postgresql: acidv1.Postgresql{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cluster",
						Namespace: "default",
					},
					Spec: acidv1.PostgresSpec{
						TeamID:           "test-team",
						NumberOfInstances: tt.numberOfInstances,
						Volume:           acidv1.Volume{Size: "1Gi"},
					},
					Status: acidv1.PostgresStatus{
						PostgresClusterStatus: tt.currentStatus,
					},
				},
				KubeClient:     *kubeClient,
				ConnectionPooler: tt.poolerObjs,
				logger:         lifecycleLogger,
				eventRecorder:  lifecycleEventRecorder,
			}

			newSpec := c.DeepCopy()
			if tt.lifecyclePhase != "" {
				newSpec.Spec.Lifecycle = &acidv1.LifecycleSpec{Phase: tt.lifecyclePhase}
			}

			handled, err := c.handleHibernateAndWakeUp(newSpec)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.wantHandled, handled)
		})
	}
}

func TestHandleHibernateAndWakeUp_WakeUp(t *testing.T) {
	tests := []struct {
		name                    string
		currentStatus           string
		numberOfInstances       int32
		previousNumberOfInst     int32
		previousPoolerInstances  map[string]int32
		lifecyclePhase           string
		k8sUpdateSucceeds        bool
		k8sStatusSucceeds        bool
		poolerPatchSucceeds      bool
		wantHandled             bool
		wantErr                 bool
		errContains             string
	}{
		{
			name:                   "Stopped + lifecycle cleared - wake-up",
			currentStatus:          "Stopped",
			numberOfInstances:      0,
			previousNumberOfInst:    3,
			previousPoolerInstances: map[string]int32{"master": 2, "replica": 0},
			lifecyclePhase:          "",
			k8sUpdateSucceeds:      true,
			k8sStatusSucceeds:     true,
			wantHandled:            true,
			wantErr:                false,
		},
		{
			name:                   "Stopped + previousNumberOfInstances is 0",
			currentStatus:          "Stopped",
			numberOfInstances:      0,
			previousNumberOfInst:    0,
			lifecyclePhase:          "",
			wantHandled:            false, // No restore possible
			wantErr:                false,
		},
		{
			name:                   "Stopped + lifecycle cleared + K8s update fails",
			currentStatus:          "Stopped",
			numberOfInstances:      0,
			previousNumberOfInst:    3,
			lifecyclePhase:          "",
			k8sUpdateSucceeds:      false,
			wantHandled:            false,
			wantErr:                true,
			errContains:            "could not update spec during wake-up",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kubeClient, _, acidClientSet := newFakeK8sClientForLifecycle()

			if tt.k8sUpdateSucceeds {
				acidClientSet.PrependReactor("update", "postgresqls", func(action k8stesting.Action) (bool, runtime.Object, error) {
					updateAction := action.(k8stesting.UpdateAction)
					pg := updateAction.GetObject().(*acidv1.Postgresql)
					return true, pg, nil
				})
			}

			c := &Cluster{
				Config: Config{
					OpConfig: config.Config{
						PodManagementPolicy: "ordered_ready",
					},
				},
				Postgresql: acidv1.Postgresql{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cluster",
						Namespace: "default",
					},
					Spec: acidv1.PostgresSpec{
						TeamID:           "test-team",
						NumberOfInstances: tt.numberOfInstances,
						Volume:           acidv1.Volume{Size: "1Gi"},
					},
					Status: acidv1.PostgresStatus{
						PostgresClusterStatus:      tt.currentStatus,
						PreviousNumberOfInstances:  tt.previousNumberOfInst,
						PreviousPoolerInstances:     tt.previousPoolerInstances,
					},
				},
				KubeClient:    *kubeClient,
				logger:        lifecycleLogger,
				eventRecorder: lifecycleEventRecorder,
			}

			newSpec := c.DeepCopy()

			handled, err := c.handleHibernateAndWakeUp(newSpec)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.wantHandled, handled)
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
				KubeClient:      *kubeClient,
				ConnectionPooler: tt.poolerObjs,
				logger:          lifecycleLogger,
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
		},
		{
			name:     "Restore master to 2",
			prevInst: map[string]int32{"master": 2},
		},
		{
			name:     "Restore both roles",
			prevInst: map[string]int32{"master": 2, "replica": 1},
		},
		{
			name:     "Restore master to 0 (keep at 0)",
			prevInst: map[string]int32{"master": 0},
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

			assert.NoError(t, err)
		})
	}
}

func TestLifecycleStateTransitions(t *testing.T) {
	t.Run("complete Hibernate flow: Running -> Stopping -> Stopped", func(t *testing.T) {
		c := &Cluster{
			logger: lifecycleLogger,
			Postgresql: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			},
		}

		// Initial: Running
		oldSpec := acidv1.Postgresql{
			Status: acidv1.PostgresStatus{PostgresClusterStatus: "Running"},
		}
		newSpec := &acidv1.Postgresql{
			Spec: acidv1.PostgresSpec{
				NumberOfInstances: 3,
				Lifecycle:         &acidv1.LifecycleSpec{Phase: "stopped"},
			},
			Status: acidv1.PostgresStatus{PostgresClusterStatus: "Running"},
		}

		// Step 1: manageHibernateState should initiate hibernate
		continueSync := c.manageHibernateState(oldSpec, newSpec)
		assert.True(t, continueSync)
		assert.Equal(t, int32(0), newSpec.Spec.NumberOfInstances)
		assert.Equal(t, "Stopping", newSpec.Status.PostgresClusterStatus)
		assert.Equal(t, int32(3), newSpec.Status.PreviousNumberOfInstances)
	})

	t.Run("complete Wake-up flow: Stopped -> Updating -> Running", func(t *testing.T) {
		c := &Cluster{
			logger: lifecycleLogger,
			Postgresql: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			},
		}

		// After hibernate: Stopped, replicas = 0
		c.Statefulset = &appsv1.StatefulSet{
			Spec: appsv1.StatefulSetSpec{Replicas: int32Ptr(0)},
		}

		oldSpec := acidv1.Postgresql{
			Status: acidv1.PostgresStatus{PostgresClusterStatus: "Stopped"},
		}
		newSpec := &acidv1.Postgresql{
			Spec: acidv1.PostgresSpec{
				NumberOfInstances: 0,
				// Lifecycle cleared by user
			},
			Status: acidv1.PostgresStatus{
				PostgresClusterStatus:      "Stopped",
				PreviousNumberOfInstances: 3,
			},
		}

		// Step 1: manageHibernateState should restore
		continueSync := c.manageHibernateState(oldSpec, newSpec)
		assert.True(t, continueSync)
		assert.Equal(t, int32(3), newSpec.Spec.NumberOfInstances)
		assert.Equal(t, "Updating", newSpec.Status.PostgresClusterStatus)
	})
}

func TestLifecycleUpdateBlocksDuringStopping(t *testing.T) {
	kubeClient, _, _ := newFakeK8sClientForLifecycle()

	c := &Cluster{
		Config: Config{
			OpConfig: config.Config{},
		},
		Postgresql: acidv1.Postgresql{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			Spec: acidv1.PostgresSpec{
				NumberOfInstances: 0,
				Lifecycle:         &acidv1.LifecycleSpec{Phase: "stopped"},
			},
			Status: acidv1.PostgresStatus{
				PostgresClusterStatus: "Stopping",
			},
		},
		KubeClient: *kubeClient,
		logger:     lifecycleLogger,
	}

	newSpec := c.DeepCopy()
	blocked, err := c.blockLifecycleUpdate(newSpec)

	assert.True(t, blocked)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot update cluster while it is stopping")
}

func TestLifecycleUpdateBlocksWhenStoppedWithPhase(t *testing.T) {
	kubeClient, _, _ := newFakeK8sClientForLifecycle()

	c := &Cluster{
		Config: Config{
			OpConfig: config.Config{},
		},
		Postgresql: acidv1.Postgresql{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			Spec: acidv1.PostgresSpec{
				NumberOfInstances: 0,
				Lifecycle:         &acidv1.LifecycleSpec{Phase: "stopped"},
			},
			Status: acidv1.PostgresStatus{
				PostgresClusterStatus: "Stopped",
			},
		},
		KubeClient: *kubeClient,
		logger:     lifecycleLogger,
	}

	newSpec := c.DeepCopy()
	blocked, err := c.blockLifecycleUpdate(newSpec)

	assert.True(t, blocked)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot update cluster while stopped")
}

func TestLifecycleUpdateAllowsWakeUp(t *testing.T) {
	kubeClient, _, _ := newFakeK8sClientForLifecycle()

	c := &Cluster{
		Config: Config{
			OpConfig: config.Config{},
		},
		Postgresql: acidv1.Postgresql{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			Spec: acidv1.PostgresSpec{
				NumberOfInstances: 0,
				// Lifecycle cleared by user
			},
			Status: acidv1.PostgresStatus{
				PostgresClusterStatus:      "Stopped",
				PreviousNumberOfInstances: 3,
			},
		},
		KubeClient: *kubeClient,
		logger:     lifecycleLogger,
	}

	newSpec := c.DeepCopy()
	blocked, err := c.blockLifecycleUpdate(newSpec)

	assert.False(t, blocked)
	assert.NoError(t, err)
}

func TestManageHibernateState_EdgeCases(t *testing.T) {
	tests := []struct {
		name                  string
		statefulsetReplicas   *int32
		currentStatus         string
		newSpecLifecyclePhase string
		newSpecNumberOfInst   int32
		previousNumberOfInst  int32
		wantContinue          bool
		wantNumberOfInstances *int32
		wantStatus            string
	}{
		{
			name:                "Stopping state with nil statefulset - should not transition to Stopped",
			statefulsetReplicas: nil,
			currentStatus:       "Stopping",
			newSpecLifecyclePhase: "stopped",
			newSpecNumberOfInst:  0,
			wantContinue:         true,
			wantNumberOfInstances: nil,
			wantStatus:          "Stopping",
		},
		{
			name:                "Stopping with nil Lifecycle spec",
			statefulsetReplicas: int32Ptr(0),
			currentStatus:       "Stopping",
			newSpecLifecyclePhase: "",
			newSpecNumberOfInst:  0,
			wantContinue:         true,
			wantNumberOfInstances: nil,
			wantStatus:          "Stopped",
		},
		{
			name:                 "Running to Stopping when numberOfInstances already 0",
			statefulsetReplicas:  int32Ptr(0),
			currentStatus:        "Running",
			newSpecLifecyclePhase: "stopped",
			newSpecNumberOfInst:   0,
			wantContinue:          true,
			wantNumberOfInstances: int32Ptr(0),
			wantStatus:           "Stopping",
		},
		{
			name:                 "Stopped with previousNumberOfInstances=0 - sets Updating but warns",
			statefulsetReplicas:  int32Ptr(0),
			currentStatus:        "Stopped",
			newSpecLifecyclePhase: "",
			newSpecNumberOfInst:   0,
			previousNumberOfInst:  0,
			wantContinue:          true,
			wantNumberOfInstances: int32Ptr(0),
			wantStatus:           "Updating",
		},
		{
			name:                 "Stopped with nil Lifecycle - wake-up",
			statefulsetReplicas:  int32Ptr(0),
			currentStatus:        "Stopped",
			newSpecLifecyclePhase: "",
			newSpecNumberOfInst:   0,
			previousNumberOfInst:  3,
			wantContinue:          true,
			wantNumberOfInstances: int32Ptr(3),
			wantStatus:           "Updating",
		},
		{
			name:                 "Stopped with empty Lifecycle phase - wake-up",
			statefulsetReplicas:  int32Ptr(0),
			currentStatus:        "Stopped",
			newSpecLifecyclePhase: "",
			newSpecNumberOfInst:   0,
			previousNumberOfInst:  2,
			wantContinue:          true,
			wantNumberOfInstances: int32Ptr(2),
			wantStatus:           "Updating",
		},
		{
			name:                 "Stopped but lifecycle still 'stopped' - skip sync",
			statefulsetReplicas:  int32Ptr(0),
			currentStatus:        "Stopped",
			newSpecLifecyclePhase: "stopped",
			newSpecNumberOfInst:   0,
			previousNumberOfInst:  3,
			wantContinue:          false,
			wantNumberOfInstances: nil,
			wantStatus:           "Stopped",
		},
		{
			name:                 "Running without lifecycle change - normal update",
			statefulsetReplicas:  int32Ptr(3),
			currentStatus:        "Running",
			newSpecLifecyclePhase: "",
			newSpecNumberOfInst:   3,
			previousNumberOfInst:  0,
			wantContinue:          true,
			wantNumberOfInstances: int32Ptr(3),
			wantStatus:           "Running",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Cluster{
				logger: lifecycleLogger,
				Postgresql: acidv1.Postgresql{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cluster",
						Namespace: "default",
					},
				},
			}

			if tt.statefulsetReplicas != nil {
				c.Statefulset = &appsv1.StatefulSet{
					Spec: appsv1.StatefulSetSpec{
						Replicas: tt.statefulsetReplicas,
					},
				}
			}

			oldSpec := acidv1.Postgresql{
				Status: acidv1.PostgresStatus{
					PostgresClusterStatus: tt.currentStatus,
				},
			}

			newSpec := &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					NumberOfInstances: tt.newSpecNumberOfInst,
				},
				Status: acidv1.PostgresStatus{
					PostgresClusterStatus:      tt.currentStatus,
					PreviousNumberOfInstances: tt.previousNumberOfInst,
				},
			}

			if tt.newSpecLifecyclePhase != "" || tt.newSpecLifecyclePhase == "" && tt.currentStatus == "Stopped" {
				newSpec.Spec.Lifecycle = &acidv1.LifecycleSpec{
					Phase: tt.newSpecLifecyclePhase,
				}
			}

			gotContinue := c.manageHibernateState(oldSpec, newSpec)

			assert.Equal(t, tt.wantContinue, gotContinue, "continue sync mismatch")
			if tt.wantNumberOfInstances != nil {
				assert.Equal(t, *tt.wantNumberOfInstances, newSpec.Spec.NumberOfInstances, "numberOfInstances mismatch")
			}
			assert.Equal(t, tt.wantStatus, newSpec.Status.PostgresClusterStatus, "status mismatch")
		})
	}
}

func TestManageHibernateState_StateTransitionSequence(t *testing.T) {
	t.Run("Running -> Stopping -> Stopped sequence", func(t *testing.T) {
		c := &Cluster{
			logger: lifecycleLogger,
			Postgresql: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			},
		}

		oldSpec := acidv1.Postgresql{
			Status: acidv1.PostgresStatus{PostgresClusterStatus: "Running"},
		}
		newSpec := &acidv1.Postgresql{
			Spec: acidv1.PostgresSpec{
				NumberOfInstances: 3,
				Lifecycle:         &acidv1.LifecycleSpec{Phase: "stopped"},
			},
			Status: acidv1.PostgresStatus{PostgresClusterStatus: "Running"},
		}

		continueSync := c.manageHibernateState(oldSpec, newSpec)
		assert.True(t, continueSync)
		assert.Equal(t, int32(0), newSpec.Spec.NumberOfInstances)
		assert.Equal(t, int32(3), newSpec.Status.PreviousNumberOfInstances)
		assert.Equal(t, "Stopping", newSpec.Status.PostgresClusterStatus)

		c.Statefulset = &appsv1.StatefulSet{
			Spec: appsv1.StatefulSetSpec{Replicas: int32Ptr(2)},
		}
		oldSpec = acidv1.Postgresql{
			Status: acidv1.PostgresStatus{PostgresClusterStatus: "Stopping"},
		}
		newSpec.Status.PostgresClusterStatus = "Stopping"

		continueSync = c.manageHibernateState(oldSpec, newSpec)
		assert.True(t, continueSync)
		assert.Equal(t, "Stopping", newSpec.Status.PostgresClusterStatus)

		c.Statefulset.Spec.Replicas = int32Ptr(0)
		continueSync = c.manageHibernateState(oldSpec, newSpec)
		assert.True(t, continueSync)
		assert.Equal(t, "Stopped", newSpec.Status.PostgresClusterStatus)
	})

	t.Run("Stopped -> Updating -> Running sequence", func(t *testing.T) {
		c := &Cluster{
			logger: lifecycleLogger,
			Postgresql: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			},
		}

		oldSpec := acidv1.Postgresql{
			Status: acidv1.PostgresStatus{PostgresClusterStatus: "Stopped"},
		}
		newSpec := &acidv1.Postgresql{
			Spec: acidv1.PostgresSpec{
				NumberOfInstances: 0,
			},
			Status: acidv1.PostgresStatus{
				PostgresClusterStatus:      "Stopped",
				PreviousNumberOfInstances: 3,
			},
		}

		continueSync := c.manageHibernateState(oldSpec, newSpec)
		assert.True(t, continueSync)
		assert.Equal(t, int32(3), newSpec.Spec.NumberOfInstances)
		assert.Equal(t, "Updating", newSpec.Status.PostgresClusterStatus)

		oldSpec = acidv1.Postgresql{
			Status: acidv1.PostgresStatus{PostgresClusterStatus: "Updating"},
		}
		newSpec.Status.PostgresClusterStatus = "Updating"

		continueSync = c.manageHibernateState(oldSpec, newSpec)
		assert.True(t, continueSync)
		assert.Equal(t, int32(3), newSpec.Spec.NumberOfInstances)
	})
}