package controller

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	acidv1 "github.com/zalando/postgres-operator/pkg/apis/acid.zalan.do/v1"
	"github.com/zalando/postgres-operator/pkg/cluster"
	fakeacidv1 "github.com/zalando/postgres-operator/pkg/generated/clientset/versioned/fake"
	"github.com/zalando/postgres-operator/pkg/spec"
	"github.com/zalando/postgres-operator/pkg/util/k8sutil"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

var (
	True  = true
	False = false
)

func newPostgresqlTestController() *Controller {
	controller := NewController(&spec.ControllerConfig{}, "postgresql-test")
	return controller
}

var postgresqlTestController = newPostgresqlTestController()

func TestControllerOwnershipOnPostgresql(t *testing.T) {
	tests := []struct {
		name  string
		pg    *acidv1.Postgresql
		owned bool
		error string
	}{
		{
			"Postgres cluster with defined ownership of mocked controller",
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{"acid.zalan.do/controller": "postgresql-test"},
				},
			},
			True,
			"Postgres cluster should be owned by operator, but controller says no",
		},
		{
			"Postgres cluster with defined ownership of another controller",
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{"acid.zalan.do/controller": "stups-test"},
				},
			},
			False,
			"Postgres cluster should be owned by another operator, but controller say yes",
		},
		{
			"Test Postgres cluster without defined ownership",
			&acidv1.Postgresql{},
			False,
			"Postgres cluster should be owned by operator with empty controller ID, but controller says yes",
		},
	}
	for _, tt := range tests {
		if postgresqlTestController.hasOwnership(tt.pg) != tt.owned {
			t.Errorf("%s: %v", tt.name, tt.error)
		}
	}
}

func TestMergeDeprecatedPostgreSQLSpecParameters(t *testing.T) {
	tests := []struct {
		name  string
		in    *acidv1.PostgresSpec
		out   *acidv1.PostgresSpec
		error string
	}{
		{
			"Check that old parameters propagate values to the new ones",
			&acidv1.PostgresSpec{UseLoadBalancer: &True, ReplicaLoadBalancer: &True},
			&acidv1.PostgresSpec{UseLoadBalancer: nil, ReplicaLoadBalancer: nil,
				EnableMasterLoadBalancer: &True, EnableReplicaLoadBalancer: &True},
			"New parameters should be set from the values of old ones",
		},
		{
			"Check that new parameters are not set when both old and new ones are present",
			&acidv1.PostgresSpec{UseLoadBalancer: &True, EnableMasterLoadBalancer: &False},
			&acidv1.PostgresSpec{UseLoadBalancer: nil, EnableMasterLoadBalancer: &False},
			"New parameters should remain unchanged when both old and new are present",
		},
	}
	for _, tt := range tests {
		result := postgresqlTestController.mergeDeprecatedPostgreSQLSpecParameters(tt.in)
		if !reflect.DeepEqual(result, tt.out) {
			t.Errorf("%s: %v", tt.name, tt.error)
		}
	}
}

func TestMeetsClusterDeleteAnnotations(t *testing.T) {
	// set delete annotations in configuration
	postgresqlTestController.opConfig.DeleteAnnotationDateKey = "delete-date"
	postgresqlTestController.opConfig.DeleteAnnotationNameKey = "delete-clustername"

	currentTime := time.Now()
	today := currentTime.Format("2006-01-02") // go's reference date
	clusterName := "acid-test-cluster"

	tests := []struct {
		name  string
		pg    *acidv1.Postgresql
		error string
	}{
		{
			"Postgres cluster with matching delete annotations",
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name: clusterName,
					Annotations: map[string]string{
						"delete-date":        today,
						"delete-clustername": clusterName,
					},
				},
			},
			"",
		},
		{
			"Postgres cluster with violated delete date annotation",
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name: clusterName,
					Annotations: map[string]string{
						"delete-date":        "2020-02-02",
						"delete-clustername": clusterName,
					},
				},
			},
			fmt.Sprintf("annotation delete-date not matching the current date: got 2020-02-02, expected %s", today),
		},
		{
			"Postgres cluster with violated delete cluster name annotation",
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name: clusterName,
					Annotations: map[string]string{
						"delete-date":        today,
						"delete-clustername": "acid-minimal-cluster",
					},
				},
			},
			fmt.Sprintf("annotation delete-clustername not matching the cluster name: got acid-minimal-cluster, expected %s", clusterName),
		},
		{
			"Postgres cluster with missing delete annotations",
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:        clusterName,
					Annotations: map[string]string{},
				},
			},
			"annotation delete-date not set in manifest to allow cluster deletion",
		},
		{
			"Postgres cluster with missing delete cluster name annotation",
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name: clusterName,
					Annotations: map[string]string{
						"delete-date": today,
					},
				},
			},
			"annotation delete-clustername not set in manifest to allow cluster deletion",
		},
	}
	for _, tt := range tests {
		if err := postgresqlTestController.meetsClusterDeleteAnnotations(tt.pg); err != nil {
			if !reflect.DeepEqual(err.Error(), tt.error) {
				t.Errorf("Expected error %q, got: %v", tt.error, err)
			}
		}
	}
}

func TestCleanupRestores(t *testing.T) {
	namespace := "default"
	tests := []struct {
		name                string
		configMaps          []*v1.ConfigMap
		retention           time.Duration
		remainingConfigMaps int
		err                 error
	}{
		{
			"no config maps to delete",
			[]*v1.ConfigMap{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "pitr-state-test-1",
						Namespace:         namespace,
						CreationTimestamp: metav1.Now(),
					},
				},
			},
			24 * time.Hour,
			1,
			nil,
		},
		{
			"one config map to delete",
			[]*v1.ConfigMap{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "pitr-state-test-1",
						Namespace:         namespace,
						CreationTimestamp: metav1.NewTime(time.Now().Add(-48 * time.Hour)),
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "pitr-state-test-2",
						Namespace:         namespace,
						CreationTimestamp: metav1.Now(),
					},
				},
			},
			24 * time.Hour,
			1,
			nil,
		},
		{
			"do not delete non-pitr config maps",
			[]*v1.ConfigMap{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "test-1",
						Namespace:         namespace,
						CreationTimestamp: metav1.NewTime(time.Now().Add(-48 * time.Hour)),
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "pitr-state-test-2",
						Namespace:         namespace,
						CreationTimestamp: metav1.Now(),
					},
				},
			},
			24 * time.Hour,
			2,
			nil,
		},
		{
			"zero retention, do nothing",
			[]*v1.ConfigMap{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "pitr-state-test-1",
						Namespace:         namespace,
						CreationTimestamp: metav1.NewTime(time.Now().Add(-48 * time.Hour)),
					},
				},
			},
			0,
			1,
			nil,
		},
		{
			"list config maps fails",
			[]*v1.ConfigMap{},
			24 * time.Hour,
			0,
			fmt.Errorf("synthetic list error"),
		},
		{
			"delete config map fails",
			[]*v1.ConfigMap{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "pitr-state-test-to-delete",
						Namespace:         namespace,
						CreationTimestamp: metav1.NewTime(time.Now().Add(-48 * time.Hour)),
					},
				},
			},
			24 * time.Hour,
			1,
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newPostgresqlTestController()
			c.opConfig.PitrBackupRetention = tt.retention
			c.opConfig.WatchedNamespace = namespace

			client := fake.NewSimpleClientset()

			if tt.name == "list config maps fails" {
				client.PrependReactor("list", "configmaps", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
					return true, nil, fmt.Errorf("synthetic list error")
				})
			}
			if tt.name == "delete config map fails" {
				client.PrependReactor("delete", "configmaps", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
					return true, nil, fmt.Errorf("synthetic delete error")
				})
			}

			c.KubeClient = k8sutil.KubernetesClient{
				ConfigMapsGetter: client.CoreV1(),
			}

			for _, cm := range tt.configMaps {
				_, err := c.KubeClient.ConfigMaps(namespace).Create(context.TODO(), cm, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("Could not create config map: %v", err)
				}
			}

			err := c.cleanupRestores()

			if err != nil {
				if tt.err == nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if !strings.Contains(err.Error(), tt.err.Error()) {
					t.Fatalf("error mismatch: got %q, expected to contain %q", err, tt.err)
				}
			} else if tt.err != nil {
				t.Fatalf("expected error %q, but got none", tt.err)
			}

			if tt.name != "list config maps fails" {
				cms, err := c.KubeClient.ConfigMaps(namespace).List(context.TODO(), metav1.ListOptions{})
				if err != nil {
					t.Fatalf("Could not list config maps: %v", err)
				}
				if len(cms.Items) != tt.remainingConfigMaps {
					t.Errorf("expected %d config maps, got %d", tt.remainingConfigMaps, len(cms.Items))
				}
			}
		})
	}
}

func TestProcessSingleInProgressCm(t *testing.T) {
	tests := []struct {
		name                string
		cm                  v1.ConfigMap
		err                 string
		expectedPgName      string
		expectedPgNamespace string
	}{
		{
			"json marshal error",
			v1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pitr-state-test-1",
					Namespace: "test",
				},
				Data: map[string]string{
					cluster.PitrSpecDataKey: "invalid json",
				},
			},
			"could not unmarshal postgresql spec from ConfigMap \"pitr-state-test-1\"",
			"",
			"",
		},
		{
			"successful create",
			v1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pitr-state-test-1",
					Namespace: "test",
				},
				Data: map[string]string{
					cluster.PitrSpecDataKey: "{\"apiVersion\":\"acid.zalan.do/v1\",\"kind\":\"postgresql\",\"metadata\":{\"name\":\"acid-minimal-cluster\",\"namespace\":\"po\"},\"spec\":{\"teamId\":\"acid\",\"volume\":{\"size\":\"1Gi\"},\"numberOfInstances\":1,\"users\":{\"zalando\":[\"superuser\",\"createdb\"]},\"databases\":{\"foo\":\"zalando\"},\"postgresql\":{\"version\":\"16\"},\"enableLogicalBackup\":false,\"patroni\":{\"pg_hba\":[\"local all all trust\",\"host all all 0.0.0.0/0 md5\",\"host replication all 0.0.0.0/0 md5\"]}}}",
				},
			},
			"",
			"acid-minimal-cluster",
			"po",
		},
		{
			"postgresql resource already exists",
			v1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pitr-state-test-1",
					Namespace: "test",
				},
				Data: map[string]string{
					cluster.PitrSpecDataKey: "{\"apiVersion\":\"acid.zalan.do/v1\",\"kind\":\"postgresql\",\"metadata\":{\"name\":\"acid-minimal-cluster\",\"namespace\":\"po\"},\"spec\":{\"teamId\":\"acid\",\"volume\":{\"size\":\"1Gi\"},\"numberOfInstances\":1,\"users\":{\"zalando\":[\"superuser\",\"createdb\"]},\"databases\":{\"foo\":\"zalando\"},\"postgresql\":{\"version\":\"16\"}}}",
				},
			},
			"",
			"acid-minimal-cluster",
			"po",
		},
		{
			"spec with missing teamId",
			v1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pitr-state-test-2",
					Namespace: "test",
				},
				Data: map[string]string{
					cluster.PitrSpecDataKey: "{\"apiVersion\":\"acid.zalan.do/v1\",\"kind\":\"postgresql\",\"metadata\":{\"name\":\"acid-spec-without-teamid\",\"namespace\":\"po\"},\"spec\":{\"volume\":{\"size\":\"1Gi\"},\"numberOfInstances\":1,\"users\":{\"zalando\":[\"superuser\",\"createdb\"]},\"databases\":{\"foo\":\"zalando\"}}}",
				},
			},
			"",
			"acid-spec-without-teamid",
			"po",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newPostgresqlTestController()
			acidClientSet := fakeacidv1.NewSimpleClientset()

			if tt.name == "postgresql resource already exists" {
				pg := &acidv1.Postgresql{
					ObjectMeta: metav1.ObjectMeta{
						Name:      tt.expectedPgName,
						Namespace: tt.expectedPgNamespace,
					},
					Spec: acidv1.PostgresSpec{
						TeamID: "some-other-team",
					},
				}
				_, err := acidClientSet.AcidV1().Postgresqls(tt.expectedPgNamespace).Create(context.TODO(), pg, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("could not pre-create postgresql resource for test: %v", err)
				}
			}

			c.KubeClient = k8sutil.KubernetesClient{
				PostgresqlsGetter: acidClientSet.AcidV1(),
			}

			err := c.processSingleInProgressCm(tt.cm)

			if err != nil {
				if tt.err == "" {
					t.Fatalf("unexpected error: %v", err)
				}
				if !strings.Contains(err.Error(), tt.err) {
					t.Fatalf("errors does not match, actual err: %v, expected err: %v", err, tt.err)
				}
			} else if tt.err != "" {
				t.Fatalf("expected error containing %q, but got no error", tt.err)
			}

			if tt.err == "" && tt.expectedPgName != "" {
				pg, err := acidClientSet.AcidV1().Postgresqls(tt.expectedPgNamespace).Get(context.TODO(), tt.expectedPgName, metav1.GetOptions{})
				if err != nil {
					t.Fatalf("could not get postgresql resource: %v", err)
				}

				switch tt.name {
				case "successful create":
					if pg.Spec.TeamID != "acid" {
						t.Errorf("expected teamId 'acid', got '%s'", pg.Spec.TeamID)
					}
				case "postgresql resource already exists":
					if pg.Spec.TeamID != "some-other-team" {
						t.Errorf("expected teamId to be 'some-other-team', but it was overwritten to '%s'", pg.Spec.TeamID)
					}
				case "spec with missing teamId":
					if pg.Spec.TeamID != "" {
						t.Errorf("expected teamId to be empty, got '%s'", pg.Spec.TeamID)
					}
				}
			}
		})
	}
}

func TestProcessInProgressCm(t *testing.T) {
	tests := []struct {
		name                string
		namespace           string
		cms                 []*v1.ConfigMap
		err                 string
		expectedPgCreations int
	}{
		{
			"process one of two in-progress cms",
			"po",
			[]*v1.ConfigMap{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pitr-state-test-1",
						Namespace: "po",
						Labels: map[string]string{
							cluster.PitrStateLabelKey: cluster.PitrStateLabelValueInProgress,
						},
					},
					Data: map[string]string{
						cluster.PitrSpecDataKey: "{\"metadata\":{\"name\":\"acid-test-cluster-1\"}}",
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pitr-state-test-2",
						Namespace: "po",
						Labels: map[string]string{
							cluster.PitrStateLabelKey: cluster.PitrStateLabelValuePending,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pitr-state-test-3",
						Namespace: "po",
					},
				},
			},
			"",
			1,
		},
		{
			"list fails",
			"po",
			[]*v1.ConfigMap{},
			"synthetic list error",
			0,
		},
		{
			"single cm process fails",
			"po",
			[]*v1.ConfigMap{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pitr-state-good",
						Namespace: "po",
						Labels: map[string]string{
							cluster.PitrStateLabelKey: cluster.PitrStateLabelValueInProgress,
						},
					},
					Data: map[string]string{
						cluster.PitrSpecDataKey: "{\"metadata\":{\"name\":\"acid-good-cluster\"}}",
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pitr-state-bad",
						Namespace: "po",
						Labels: map[string]string{
							cluster.PitrStateLabelKey: cluster.PitrStateLabelValueInProgress,
						},
					},
					Data: map[string]string{
						cluster.PitrSpecDataKey: "invalid-json",
					},
				},
			},
			"",
			1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newPostgresqlTestController()
			clientSet := fake.NewSimpleClientset()
			acidClientSet := fakeacidv1.NewSimpleClientset()

			if tt.name == "list fails" {
				clientSet.PrependReactor("list", "configmaps", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
					return true, nil, fmt.Errorf("synthetic list error")
				})
			}

			c.KubeClient = k8sutil.KubernetesClient{
				ConfigMapsGetter:  clientSet.CoreV1(),
				PostgresqlsGetter: acidClientSet.AcidV1(),
			}

			for _, cm := range tt.cms {
				_, err := c.KubeClient.ConfigMaps(tt.namespace).Create(context.TODO(), cm, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("Could not create config map: %v", err)
				}
			}

			err := c.processInProgressCm(tt.namespace)
			if err != nil {
				if tt.err == "" {
					t.Fatalf("unexpected error: %v", err)
				}
				if !strings.Contains(err.Error(), tt.err) {
					t.Fatalf("errors does not match, actual err: %v, expected err: %v", err, tt.err)
				}
			} else if tt.err != "" {
				t.Fatalf("expected error containing %q, but got no error", tt.err)
			}

			var creations int
			for _, action := range acidClientSet.Actions() {
				if action.GetVerb() == "create" && action.GetResource().Resource == "postgresqls" {
					creations++
				}
			}

			if creations != tt.expectedPgCreations {
				t.Errorf("expected %d postgresql resources to be created, but found %d", tt.expectedPgCreations, creations)
			}
		})
	}
}

func TestProcessSinglePendingCm(t *testing.T) {
	tests := []struct {
		name          string
		cm            v1.ConfigMap
		pgExists      bool
		getPgFails    bool
		updateCmFails bool
		expectedErr   string
		expectedLabel string
	}{
		{
			"postgresql cr still exists",
			v1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pitr-state-test-cluster",
					Namespace: "default",
					Labels: map[string]string{
						cluster.PitrStateLabelKey: cluster.PitrStateLabelValuePending,
					},
				},
			},
			true,
			false,
			false,
			"",
			cluster.PitrStateLabelValuePending,
		},
		{
			"get postgresql cr fails",
			v1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pitr-state-test-cluster",
					Namespace: "default",
					Labels: map[string]string{
						cluster.PitrStateLabelKey: cluster.PitrStateLabelValuePending,
					},
				},
			},
			false,
			true,
			false,
			"could not check for existence of Postgresql CR",
			cluster.PitrStateLabelValuePending,
		},
		{
			"postgresql cr does not exist, cm update succeeds",
			v1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pitr-state-test-cluster",
					Namespace: "default",
					Labels: map[string]string{
						cluster.PitrStateLabelKey: cluster.PitrStateLabelValuePending,
					},
				},
			},
			false,
			false,
			false,
			"",
			cluster.PitrStateLabelValueInProgress,
		},
		{
			"postgresql cr does not exist, cm update fails",
			v1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pitr-state-test-cluster",
					Namespace: "default",
					Labels: map[string]string{
						cluster.PitrStateLabelKey: cluster.PitrStateLabelValuePending,
					},
				},
			},
			false,
			false,
			true,
			"could not update ConfigMap",
			cluster.PitrStateLabelValuePending,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newPostgresqlTestController()
			clientSet := fake.NewSimpleClientset(&tt.cm)
			acidClientSet := fakeacidv1.NewSimpleClientset()

			if tt.pgExists {
				pg := &acidv1.Postgresql{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cluster",
						Namespace: "default",
					},
				}
				_, err := acidClientSet.AcidV1().Postgresqls("default").Create(context.TODO(), pg, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("could not create postgresql resource: %v", err)
				}
			}

			if tt.getPgFails {
				acidClientSet.PrependReactor("get", "postgresqls", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
					return true, nil, fmt.Errorf("synthetic get error")
				})
			}

			if tt.updateCmFails {
				clientSet.PrependReactor("update", "configmaps", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
					return true, nil, fmt.Errorf("synthetic update error")
				})
			}

			c.KubeClient = k8sutil.KubernetesClient{
				ConfigMapsGetter:  clientSet.CoreV1(),
				PostgresqlsGetter: acidClientSet.AcidV1(),
			}

			err := c.processSinglePendingCm(tt.cm)

			if err != nil {
				if tt.expectedErr == "" {
					t.Fatalf("unexpected error: %v", err)
				}
				if !strings.Contains(err.Error(), tt.expectedErr) {
					t.Fatalf("error mismatch: got %q, expected to contain %q", err, tt.expectedErr)
				}
			} else if tt.expectedErr != "" {
				t.Fatalf("expected error containing %q, but got no error", tt.expectedErr)
			}

			if !tt.updateCmFails {
				updatedCm, err := clientSet.CoreV1().ConfigMaps("default").Get(context.TODO(), "pitr-state-test-cluster", metav1.GetOptions{})
				if err != nil {
					t.Fatalf("could not get configmap: %v", err)
				}
				if updatedCm.Labels[cluster.PitrStateLabelKey] != tt.expectedLabel {
					t.Errorf("expected label %q but got %q", tt.expectedLabel, updatedCm.Labels[cluster.PitrStateLabelKey])
				}
			}
		})
	}
}

func TestProcessPendingCm(t *testing.T) {
	tests := []struct {
		name                     string
		namespace                string
		cms                      []*v1.ConfigMap
		listFails                bool
		err                      string
		expectedProcessedPending int
	}{
		{
			"process one of two pending cms",
			"default",
			[]*v1.ConfigMap{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pitr-state-test-1",
						Namespace: "default",
						Labels: map[string]string{
							cluster.PitrStateLabelKey: cluster.PitrStateLabelValuePending,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pitr-state-test-2",
						Namespace: "default",
						Labels: map[string]string{
							cluster.PitrStateLabelKey: cluster.PitrStateLabelValueInProgress,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pitr-state-test-3",
						Namespace: "default",
					},
				},
			},
			false,
			"",
			1,
		},
		{
			"no pending cms to process",
			"default",
			[]*v1.ConfigMap{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pitr-state-test-1",
						Namespace: "default",
						Labels: map[string]string{
							cluster.PitrStateLabelKey: cluster.PitrStateLabelValueInProgress,
						},
					},
				},
			},
			false,
			"",
			0,
		},
		{
			"list fails",
			"default",
			[]*v1.ConfigMap{},
			true,
			"could not list pending restore ConfigMaps",
			0,
		},
		{
			"process multiple pending cms",
			"default",
			[]*v1.ConfigMap{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pitr-state-test-1",
						Namespace: "default",
						Labels: map[string]string{
							cluster.PitrStateLabelKey: cluster.PitrStateLabelValuePending,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pitr-state-test-2",
						Namespace: "default",
						Labels: map[string]string{
							cluster.PitrStateLabelKey: cluster.PitrStateLabelValuePending,
						},
					},
				},
			},
			false,
			"",
			2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newPostgresqlTestController()
			clientSet := fake.NewSimpleClientset()
			acidClientSet := fakeacidv1.NewSimpleClientset()

			if tt.listFails {
				clientSet.PrependReactor("list", "configmaps", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
					return true, nil, fmt.Errorf("synthetic list error")
				})
			}

			c.KubeClient = k8sutil.KubernetesClient{
				ConfigMapsGetter:  clientSet.CoreV1(),
				PostgresqlsGetter: acidClientSet.AcidV1(),
			}

			for _, cm := range tt.cms {
				_, err := c.KubeClient.ConfigMaps(tt.namespace).Create(context.TODO(), cm, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("Could not create config map: %v", err)
				}
			}

			err := c.processPendingCm(tt.namespace)
			if err != nil {
				if tt.err == "" {
					t.Fatalf("unexpected error: %v", err)
				}
				if !strings.Contains(err.Error(), tt.err) {
					t.Fatalf("errors does not match, actual err: %v, expected err: %v", err, tt.err)
				}
			} else if tt.err != "" {
				t.Fatalf("expected error containing %q, but got no error", tt.err)
			}

			if !tt.listFails {
				var pendingProcessed int
				for _, action := range acidClientSet.Actions() {
					if action.GetVerb() == "get" && action.GetResource().Resource == "postgresqls" {
						pendingProcessed++
					}
				}

				if pendingProcessed != tt.expectedProcessedPending {
					t.Errorf("expected %d pending cms to be processed, but found %d", tt.expectedProcessedPending, pendingProcessed)
				}
			}
		})
	}
}

func TestProcessPendingRestores(t *testing.T) {
	tests := []struct {
		name                     string
		watchedNamespace         string
		cms                      []*v1.ConfigMap
		pendingCmListFails       bool
		inProgressCmListFails    bool
		expectedErr              string
		expectedPendingProcessed int
		expectedInProgressCreate int
	}{
		{
			"process both pending and in-progress cms with watched namespace",
			"default",
			[]*v1.ConfigMap{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pitr-state-test-1",
						Namespace: "default",
						Labels: map[string]string{
							cluster.PitrStateLabelKey: cluster.PitrStateLabelValuePending,
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pitr-state-test-2",
						Namespace: "default",
						Labels: map[string]string{
							cluster.PitrStateLabelKey: cluster.PitrStateLabelValueInProgress,
						},
					},
					Data: map[string]string{
						cluster.PitrSpecDataKey: "{\"metadata\":{\"name\":\"acid-test-cluster\"}}",
					},
				},
			},
			false,
			false,
			"",
			1,
			1,
		},
		{
			"use all namespaces when watched namespace is empty",
			"",
			[]*v1.ConfigMap{},
			false,
			false,
			"",
			0,
			0,
		},
		{
			"processPendingCm fails",
			"default",
			[]*v1.ConfigMap{},
			true,
			false,
			"could not list pending restore ConfigMaps",
			0,
			0,
		},
		{
			"processInProgressCm fails",
			"default",
			[]*v1.ConfigMap{},
			false,
			true,
			"could not list in-progress restore ConfigMaps",
			0,
			0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newPostgresqlTestController()
			c.opConfig.WatchedNamespace = tt.watchedNamespace
			clientSet := fake.NewSimpleClientset()
			acidClientSet := fakeacidv1.NewSimpleClientset()

			listCallCount := 0
			if tt.pendingCmListFails || tt.inProgressCmListFails {
				clientSet.PrependReactor("list", "configmaps", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
					listCallCount++
					if tt.pendingCmListFails && listCallCount == 1 {
						return true, nil, fmt.Errorf("synthetic list error")
					}
					if tt.inProgressCmListFails && listCallCount == 2 {
						return true, nil, fmt.Errorf("synthetic list error")
					}
					return false, nil, nil
				})
			}

			c.KubeClient = k8sutil.KubernetesClient{
				ConfigMapsGetter:  clientSet.CoreV1(),
				PostgresqlsGetter: acidClientSet.AcidV1(),
			}

			namespace := tt.watchedNamespace
			if namespace == "" {
				namespace = "default"
			}
			for _, cm := range tt.cms {
				_, err := c.KubeClient.ConfigMaps(namespace).Create(context.TODO(), cm, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("Could not create config map: %v", err)
				}
			}

			err := c.processPendingRestores()
			if err != nil {
				if tt.expectedErr == "" {
					t.Fatalf("unexpected error: %v", err)
				}
				if !strings.Contains(err.Error(), tt.expectedErr) {
					t.Fatalf("error mismatch: got %q, expected to contain %q", err, tt.expectedErr)
				}
			} else if tt.expectedErr != "" {
				t.Fatalf("expected error containing %q, but got no error", tt.expectedErr)
			}

			if tt.expectedErr == "" {
				var pendingProcessed int
				var inProgressCreate int
				for _, action := range acidClientSet.Actions() {
					if action.GetVerb() == "get" && action.GetResource().Resource == "postgresqls" {
						pendingProcessed++
					}
					if action.GetVerb() == "create" && action.GetResource().Resource == "postgresqls" {
						inProgressCreate++
					}
				}

				if pendingProcessed != tt.expectedPendingProcessed {
					t.Errorf("expected %d pending cms to be processed, but found %d", tt.expectedPendingProcessed, pendingProcessed)
				}
				if inProgressCreate != tt.expectedInProgressCreate {
					t.Errorf("expected %d in-progress cms to create postgresql resources, but found %d", tt.expectedInProgressCreate, inProgressCreate)
				}
			}
		})
	}
}

func TestValidateRestoreInPlace(t *testing.T) {
	validTimestamp := time.Now().Add(-1 * time.Hour).Format(time.RFC3339)
	futureTimestamp := time.Now().Add(1 * time.Hour).Format(time.RFC3339)

	tests := []struct {
		name        string
		pgOld       *acidv1.Postgresql
		pgNew       *acidv1.Postgresql
		expectedErr string
	}{
		{
			"missing clone section",
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "acid-test-cluster",
					Namespace: "default",
				},
			},
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "acid-test-cluster",
					Namespace: "default",
				},
				Spec: acidv1.PostgresSpec{},
			},
			"'clone' section is missing in the manifest",
		},
		{
			"cluster name mismatch",
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "acid-test-cluster",
					Namespace: "default",
				},
			},
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "acid-test-cluster",
					Namespace: "default",
				},
				Spec: acidv1.PostgresSpec{
					Clone: &acidv1.CloneDescription{
						ClusterName:  "different-cluster",
						EndTimestamp: validTimestamp,
					},
				},
			},
			"clone cluster name \"different-cluster\" does not match the current cluster name \"acid-test-cluster\"",
		},
		{
			"invalid timestamp format",
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "acid-test-cluster",
					Namespace: "default",
				},
			},
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "acid-test-cluster",
					Namespace: "default",
				},
				Spec: acidv1.PostgresSpec{
					Clone: &acidv1.CloneDescription{
						ClusterName:  "acid-test-cluster",
						EndTimestamp: "invalid-timestamp",
					},
				},
			},
			"could not parse clone timestamp",
		},
		{
			"future timestamp",
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "acid-test-cluster",
					Namespace: "default",
				},
			},
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "acid-test-cluster",
					Namespace: "default",
				},
				Spec: acidv1.PostgresSpec{
					Clone: &acidv1.CloneDescription{
						ClusterName:  "acid-test-cluster",
						EndTimestamp: futureTimestamp,
					},
				},
			},
			"clone timestamp",
		},
		{
			"valid restore parameters",
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "acid-test-cluster",
					Namespace: "default",
				},
			},
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "acid-test-cluster",
					Namespace: "default",
				},
				Spec: acidv1.PostgresSpec{
					Clone: &acidv1.CloneDescription{
						ClusterName:  "acid-test-cluster",
						EndTimestamp: validTimestamp,
					},
				},
			},
			"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newPostgresqlTestController()

			err := c.validateRestoreInPlace(tt.pgOld, tt.pgNew)

			if err != nil {
				if tt.expectedErr == "" {
					t.Fatalf("unexpected error: %v", err)
				}
				if !strings.Contains(err.Error(), tt.expectedErr) {
					t.Fatalf("error mismatch: got %q, expected to contain %q", err, tt.expectedErr)
				}
			} else if tt.expectedErr != "" {
				t.Fatalf("expected error containing %q, but got no error", tt.expectedErr)
			}
		})
	}
}

func TestHandleRestoreInPlace(t *testing.T) {
	validTimestamp := time.Now().Add(-1 * time.Hour).Format(time.RFC3339)

	tests := []struct {
		name                  string
		pgOld                 *acidv1.Postgresql
		pgNew                 *acidv1.Postgresql
		cmExists              bool
		cmCreateFails         bool
		cmUpdateFails         bool
		pgDeleteFails         bool
		expectCmCreateAttempt bool
		expectCmUpdateAttempt bool
		expectPgDeleteAttempt bool
	}{
		{
			"validation fails - missing clone section",
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "acid-test-cluster",
					Namespace: "default",
				},
			},
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "acid-test-cluster",
					Namespace: "default",
				},
				Spec: acidv1.PostgresSpec{},
			},
			false,
			false,
			false,
			false,
			false,
			false,
			false,
		},
		{
			"successful restore - cm created and pg deleted",
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "acid-test-cluster",
					Namespace: "default",
				},
			},
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "acid-test-cluster",
					Namespace: "default",
				},
				Spec: acidv1.PostgresSpec{
					Clone: &acidv1.CloneDescription{
						ClusterName:  "acid-test-cluster",
						EndTimestamp: validTimestamp,
					},
				},
			},
			false,
			false,
			false,
			false,
			true,
			false,
			true,
		},
		{
			"cm already exists - cm updated and pg deleted",
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "acid-test-cluster",
					Namespace: "default",
				},
			},
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "acid-test-cluster",
					Namespace: "default",
				},
				Spec: acidv1.PostgresSpec{
					Clone: &acidv1.CloneDescription{
						ClusterName:  "acid-test-cluster",
						EndTimestamp: validTimestamp,
					},
				},
			},
			true,
			false,
			false,
			false,
			false,
			true,
			true,
		},
		{
			"cm create fails - no pg delete",
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "acid-test-cluster",
					Namespace: "default",
				},
			},
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "acid-test-cluster",
					Namespace: "default",
				},
				Spec: acidv1.PostgresSpec{
					Clone: &acidv1.CloneDescription{
						ClusterName:  "acid-test-cluster",
						EndTimestamp: validTimestamp,
					},
				},
			},
			false,
			true,
			false,
			false,
			true,
			false,
			false,
		},
		{
			"cm update fails - no pg delete",
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "acid-test-cluster",
					Namespace: "default",
				},
			},
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "acid-test-cluster",
					Namespace: "default",
				},
				Spec: acidv1.PostgresSpec{
					Clone: &acidv1.CloneDescription{
						ClusterName:  "acid-test-cluster",
						EndTimestamp: validTimestamp,
					},
				},
			},
			true,
			false,
			true,
			false,
			false,
			true,
			false,
		},
		{
			"pg delete fails",
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "acid-test-cluster",
					Namespace: "default",
				},
			},
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "acid-test-cluster",
					Namespace: "default",
				},
				Spec: acidv1.PostgresSpec{
					Clone: &acidv1.CloneDescription{
						ClusterName:  "acid-test-cluster",
						EndTimestamp: validTimestamp,
					},
				},
			},
			false,
			false,
			false,
			true,
			true,
			false,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newPostgresqlTestController()
			clientSet := fake.NewSimpleClientset()
			acidClientSet := fakeacidv1.NewSimpleClientset()

			// Pre-create postgresql resource
			_, err := acidClientSet.AcidV1().Postgresqls(tt.pgOld.Namespace).Create(context.TODO(), tt.pgOld, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("could not create postgresql resource: %v", err)
			}

			if tt.cmExists {
				cmName := fmt.Sprintf("pitr-state-%s", tt.pgNew.Name)
				cm := &v1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      cmName,
						Namespace: tt.pgNew.Namespace,
					},
				}
				_, err := clientSet.CoreV1().ConfigMaps(tt.pgNew.Namespace).Create(context.TODO(), cm, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("could not create configmap: %v", err)
				}
			}

			if tt.cmCreateFails {
				clientSet.PrependReactor("create", "configmaps", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
					return true, nil, fmt.Errorf("synthetic create error")
				})
			}

			if tt.cmUpdateFails {
				clientSet.PrependReactor("update", "configmaps", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
					return true, nil, fmt.Errorf("synthetic update error")
				})
			}

			if tt.pgDeleteFails {
				acidClientSet.PrependReactor("delete", "postgresqls", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
					return true, nil, fmt.Errorf("synthetic delete error")
				})
			}

			c.KubeClient = k8sutil.KubernetesClient{
				ConfigMapsGetter:  clientSet.CoreV1(),
				PostgresqlsGetter: acidClientSet.AcidV1(),
			}

			// Clear actions from setup phase before calling the handler
			clientSet.ClearActions()
			acidClientSet.ClearActions()

			c.handleRestoreInPlace(tt.pgOld, tt.pgNew)

			// Check ConfigMap actions (only actions from the handler itself)
			var cmCreateAttempt, cmUpdateAttempt bool
			for _, action := range clientSet.Actions() {
				if action.GetVerb() == "create" && action.GetResource().Resource == "configmaps" {
					cmCreateAttempt = true
				}
				if action.GetVerb() == "update" && action.GetResource().Resource == "configmaps" {
					cmUpdateAttempt = true
				}
			}

			// Check Postgresql actions
			var pgDeleteAttempt bool
			for _, action := range acidClientSet.Actions() {
				if action.GetVerb() == "delete" && action.GetResource().Resource == "postgresqls" {
					pgDeleteAttempt = true
				}
			}

			if cmCreateAttempt != tt.expectCmCreateAttempt {
				t.Errorf("expected cmCreateAttempt=%v, got %v", tt.expectCmCreateAttempt, cmCreateAttempt)
			}
			if cmUpdateAttempt != tt.expectCmUpdateAttempt {
				t.Errorf("expected cmUpdateAttempt=%v, got %v", tt.expectCmUpdateAttempt, cmUpdateAttempt)
			}
			if pgDeleteAttempt != tt.expectPgDeleteAttempt {
				t.Errorf("expected pgDeleteAttempt=%v, got %v", tt.expectPgDeleteAttempt, pgDeleteAttempt)
			}
		})
	}
}
