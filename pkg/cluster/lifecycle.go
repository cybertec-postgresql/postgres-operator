package cluster

import (
	"context"
	"fmt"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	acidv1 "github.com/zalando/postgres-operator/pkg/apis/acid.zalan.do/v1"
	"github.com/zalando/postgres-operator/pkg/util/k8sutil"
)

// LifecycleAction represents the detected lifecycle transition for a cluster.
// Used by both Sync (manageHibernateState) and Update (handleHibernateAndWakeUp) paths
// to determine what action to take regarding cluster hibernate/wake-up.
type LifecycleAction int

const (
	LifecycleActionNone              LifecycleAction = iota
	LifecycleActionHibernate                         // Running -> Stopping (initiate hibernate)
	LifecycleActionStoppingCompleted                 // Stopping -> Stopped (pods fully terminated)
	LifecycleActionWakeUp                            // Stopped -> Updating (initiate wake-up)
)

// detectLifecycleTransition is a pure function that examines the current and proposed specs
// and determines what lifecycle action (if any) should be taken.
//
// Detection logic:
//   - Hibernate: lifecycle.phase="stopped" + status not Stopping or Stopped
//   - Wake-up: (status == Stopped OR lifecycle cleared + has previousNumberOfInstances
//   - numberOfInstances == 0) AND new lifecycle is cleared (or nil)
func detectLifecycleTransition(
	currentStatus *acidv1.PostgresStatus,
	newSpecLifecycle *acidv1.LifecycleSpec,
	newSpecNumberOfInstances int32,
	newSpecPreviousNumberOfInstances int32,
) LifecycleAction {
	isWakingUpSimple := newSpecLifecycle == nil || newSpecLifecycle.Phase != "stopped"
	hasPreviousInstances := newSpecPreviousNumberOfInstances > 0
	needsRestore := newSpecNumberOfInstances == 0

	isWakingUp := isWakingUpSimple && hasPreviousInstances && needsRestore

	if currentStatus.Stopped() || isWakingUp {
		if isWakingUp || newSpecLifecycle == nil || newSpecLifecycle.Phase != "stopped" {
			return LifecycleActionWakeUp
		}
	}

	if newSpecLifecycle != nil &&
		newSpecLifecycle.Phase == "stopped" &&
		!currentStatus.Stopping() &&
		!currentStatus.Stopped() {
		return LifecycleActionHibernate
	}

	return LifecycleActionNone
}

// detectStoppingCompleted checks if the cluster should transition from Stopping to Stopped.
// This happens when the StatefulSet replicas have actually reached 0 (all pods terminated).
func detectStoppingCompleted(currentStatus *acidv1.PostgresStatus, statefulsetReplicas *int32) bool {
	if !currentStatus.Stopping() {
		return false
	}
	if statefulsetReplicas == nil {
		return false
	}
	return *statefulsetReplicas == 0
}

// refreshStatefulset fetches the latest StatefulSet from the API and updates c.Statefulset.
// The in-memory c.Statefulset cache is only populated by Create() and by Sync() inside
// syncStatefulSet(); after an operator restart the cache is nil, and Sync() calls
// manageHibernateState before reaching syncStatefulSet(). Without this refresh,
// detectStoppingCompleted would see stale (or nil) replica counts and never transition
// to Stopped. If the StatefulSet is not found in the API, the cache is cleared and
// nil is returned (the cluster may have been deleted).
func (c *Cluster) refreshStatefulset() error {
	sset, err := c.KubeClient.StatefulSets(c.Namespace).Get(
		context.TODO(), c.statefulSetName(), metav1.GetOptions{})
	if err != nil {
		if k8sutil.ResourceNotFound(err) {
			c.Statefulset = nil
			return nil
		}
		return fmt.Errorf("could not refresh statefulset: %w", err)
	}
	c.Statefulset = sset
	return nil
}

// checkStoppingCompleted is an I/O wrapper around detectStoppingCompleted. It refreshes
// the StatefulSet cache before delegating so callers see fresh replica counts, especially
// after an operator restart where c.Statefulset is nil. To avoid an extra API call on the
// hot Update/Sync path, the refresh is skipped when status is not Stopping (the only
// state where detectStoppingCompleted can return true).
func (c *Cluster) checkStoppingCompleted(currentStatus *acidv1.PostgresStatus) (bool, error) {
	if !currentStatus.Stopping() {
		return false, nil
	}
	if err := c.refreshStatefulset(); err != nil {
		return false, err
	}
	return detectStoppingCompleted(currentStatus, c.getStatefulsetReplicas()), nil
}

// initiateHibernate prepares the cluster for hibernation by:
// - Storing current numberOfInstances in PreviousNumberOfInstances
// - Setting numberOfInstances to 0
// - Setting status to Stopping
// - Scaling down connection pooler deployments
// - Suspending logical backup CronJob
// Errors during pooler/backup operations are logged but do not fail the transition.
func (c *Cluster) initiateHibernate(newSpec *acidv1.Postgresql) {
	newSpec.Status.PreviousNumberOfInstances = newSpec.Spec.NumberOfInstances
	newSpec.Spec.NumberOfInstances = 0
	newSpec.Status.PostgresClusterStatus = acidv1.ClusterStatusStopping

	c.logger.Infof("[lifecycle] initiating hibernate: stored previousNumberOfInstances=%d",
		newSpec.Status.PreviousNumberOfInstances)

	if err := c.scalePoolerDown(newSpec); err != nil {
		c.logger.Warningf("[lifecycle] failed to scale pooler during hibernate: %v", err)
	}

	if err := c.suspendLogicalBackupJob(); err != nil {
		c.logger.Warningf("[lifecycle] failed to suspend logical backup job: %v", err)
	}
}

// initiateWakeUp prepares the cluster for wake-up by:
// - Restoring numberOfInstances from PreviousNumberOfInstances (if > 0)
//
// - Setting status to Updating
// - Scaling up connection pooler deployments
// - Resuming logical backup CronJob
// If PreviousNumberOfInstances is 0, logs a warning but still sets status to Updating.
// Errors during pooler/backup operations are logged but do not fail the transition.
func (c *Cluster) initiateWakeUp(newSpec *acidv1.Postgresql) {
	if newSpec.Status.PreviousNumberOfInstances > 0 {
		newSpec.Spec.NumberOfInstances = newSpec.Status.PreviousNumberOfInstances
		c.logger.Infof("[lifecycle] initiating wake-up: restoring numberOfInstances=%d",
			newSpec.Status.PreviousNumberOfInstances)
	} else {
		c.logger.Warningf("[lifecycle] cluster is waking up but previousNumberOfInstances is 0, cannot restore")
	}

	newSpec.Status.PostgresClusterStatus = acidv1.ClusterStatusUpdating

	if err := c.scalePoolerUp(newSpec); err != nil {
		c.logger.Warningf("[lifecycle] failed to scale pooler during wake-up: %v", err)
	}

	if err := c.unsuspendLogicalBackupJob(); err != nil {
		c.logger.Warningf("[lifecycle] failed to resume logical backup job: %v", err)
	}
}

// persistHibernateTransition persists the hibernate transition to Kubernetes by:
// 1. Updating the PostgresCR spec (numberOfInstances=0, previousNumberOfInstances stored)
// 2. Updating the PostgresCR status (status=Stopping, previousPoolerInstances stored)
// 3. Updating the local cluster spec
// Returns (handled=true, nil) on success, (handled=false, error) on failure.
func (c *Cluster) persistHibernateTransition(newSpec *acidv1.Postgresql) (bool, error) {
	pgUpdated, err := c.KubeClient.UpdatePostgresCR(c.clusterName(), newSpec)
	if err != nil {
		return false, fmt.Errorf("could not update spec during hibernate: %w", err)
	}

	pgUpdated.Status.PreviousNumberOfInstances = newSpec.Status.PreviousNumberOfInstances
	pgUpdated.Status.PostgresClusterStatus = newSpec.Status.PostgresClusterStatus
	pgUpdated.Status.PreviousPoolerInstances = newSpec.Status.PreviousPoolerInstances

	pgUpdated, err = c.KubeClient.SetPostgresCRDStatus(c.clusterName(), pgUpdated)
	if err != nil {
		return false, fmt.Errorf("could not update status during hibernate: %w", err)
	}

	c.setSpec(pgUpdated)
	c.logger.Infof("[lifecycle] hibernate completed: cluster is stopping, numberOfInstances=0, previousNumberOfInstances=%d",
		pgUpdated.Status.PreviousNumberOfInstances)
	return true, nil
}

// persistWakeUpTransition persists the wake-up transition to Kubernetes by:
// 1. Clearing PreviousNumberOfInstances and PreviousPoolerInstances
// 2. Updating the PostgresCR spec (numberOfInstances restored from previous)
// 3. Updating the PostgresCR status (status=Updating)
// 4. Updating the local cluster spec
// Returns (handled=true, nil) on success, (handled=false, error) on failure.
func (c *Cluster) persistWakeUpTransition(newSpec *acidv1.Postgresql) (bool, error) {
	newSpec.Status.PreviousNumberOfInstances = 0
	newSpec.Status.PreviousPoolerInstances = nil

	pgUpdated, err := c.KubeClient.UpdatePostgresCR(c.clusterName(), newSpec)
	if err != nil {
		return false, fmt.Errorf("could not update spec during wake-up: %w", err)
	}

	pgUpdated.Status.PreviousNumberOfInstances = newSpec.Status.PreviousNumberOfInstances
	pgUpdated.Status.PreviousPoolerInstances = newSpec.Status.PreviousPoolerInstances
	pgUpdated.Status.PostgresClusterStatus = newSpec.Status.PostgresClusterStatus

	pgUpdated, err = c.KubeClient.SetPostgresCRDStatus(c.clusterName(), pgUpdated)
	if err != nil {
		return false, fmt.Errorf("could not update status during wake-up: %w", err)
	}

	c.setSpec(pgUpdated)
	c.logger.Infof("[lifecycle] wake-up completed: cluster is updating, numberOfInstances=%d",
		pgUpdated.Spec.NumberOfInstances)
	return true, nil
}

// persistStoppingCompletedTransition persists the Stopping->Stopped transition to Kubernetes
// by updating only the status (status=Stopped). This is called when StatefulSet replicas
// have actually reached 0.
// Returns (handled=true, nil) on success, (handled=false, error) on failure.
func (c *Cluster) persistStoppingCompletedTransition(newSpec *acidv1.Postgresql) (bool, error) {
	pgUpdated, err := c.KubeClient.SetPostgresCRDStatus(c.clusterName(), newSpec)
	if err != nil {
		return false, fmt.Errorf("could not update status during stopping completed: %w", err)
	}

	c.setSpec(pgUpdated)
	c.logger.Info("[lifecycle] stopping completed: cluster is stopped")
	return true, nil
}

// manageHibernateState handles cluster hibernate/wake-up state transitions during Sync().
// This is the Sync path - it only modifies the in-memory spec.
//
// State transitions handled:
//   - Running -> Stopping: Via initiateHibernate() (when lifecycle.phase="stopped")
//   - Stopping -> Stopped: When StatefulSet replicas reach 0 (detected here)
//   - Stopped -> Updating: Via initiateWakeUp() (when lifecycle cleared)
//
// Returns the detected LifecycleAction and a boolean indicating whether sync
// should continue (true) or return early (false, only when cluster is Stopped
// with lifecycle.phase="stopped" set).
func (c *Cluster) manageHibernateState(oldSpec acidv1.Postgresql, newSpec *acidv1.Postgresql) (LifecycleAction, bool) {
	action := detectLifecycleTransition(
		&newSpec.Status,
		newSpec.Spec.Lifecycle,
		newSpec.Spec.NumberOfInstances,
		newSpec.Status.PreviousNumberOfInstances,
	)

	switch action {
	case LifecycleActionHibernate:
		c.initiateHibernate(newSpec)
		return action, true

	case LifecycleActionWakeUp:
		c.initiateWakeUp(newSpec)
		return action, true
	}

	// Check if Stopping -> Stopped transition is needed
	done, err := c.checkStoppingCompleted(&newSpec.Status)
	if err != nil {
		// Refresh failed; treat as not-completed and let the next Sync retry.
		// Not returning the error so we don't mark the cluster SyncFailed on a
		// transient API hiccup while the cluster is mid-stop.
		c.logger.Warningf("[lifecycle] could not refresh statefulset for stopping check: %v", err)
	} else if done {
		newSpec.Status.PostgresClusterStatus = acidv1.ClusterStatusStopped
		c.logger.Info("[lifecycle] cluster has stopped, all pods are terminated")
		return LifecycleActionStoppingCompleted, true
	}

	// Skip sync if cluster is stopped and lifecycle.phase="stopped" is set
	if newSpec.Status.Stopped() && newSpec.Spec.Lifecycle != nil && newSpec.Spec.Lifecycle.Phase == "stopped" {
		return LifecycleActionNone, false
	}

	return LifecycleActionNone, true
}

// getStatefulsetReplicas returns the current replica count from the StatefulSet.
// Returns nil if StatefulSet is nil or Replicas field is nil.
func (c *Cluster) getStatefulsetReplicas() *int32 {
	if c.Statefulset == nil || c.Statefulset.Spec.Replicas == nil {
		return nil
	}
	return c.Statefulset.Spec.Replicas
}

// suspendLogicalBackupJob suspends the logical backup CronJob by setting spec.suspend=true.
// If the job was previously loaded but has been deleted externally, clears the cached reference.
// Returns nil if job is not loaded (no-op) or if job was not found (clears cache).
// Returns error only for actual failures (network errors, etc).
func (c *Cluster) suspendLogicalBackupJob() error {
	if c.LogicalBackupJob == nil {
		c.logger.Debug("logical backup job is not loaded, skipping suspend")
		return nil
	}

	// Check if job still exists (handles externally deleted jobs)
	_, err := c.KubeClient.CronJobsGetter.CronJobs(c.Namespace).Get(
		context.TODO(), c.getLogicalBackupJobName(), metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		c.logger.Info("logical backup job not found during suspend, clearing cached reference")
		c.LogicalBackupJob = nil
		return nil
	}
	if err != nil {
		return fmt.Errorf("could not get logical backup job: %w", err)
	}

	patchData := fmt.Sprintf(`{"spec":{"suspend":true}}`)
	cronJob, err := c.KubeClient.CronJobsGetter.CronJobs(c.Namespace).Patch(
		context.TODO(),
		c.getLogicalBackupJobName(),
		types.MergePatchType,
		[]byte(patchData),
		metav1.PatchOptions{},
		"",
	)
	if err != nil {
		return fmt.Errorf("could not suspend logical backup job: %w", err)
	}
	c.LogicalBackupJob = cronJob
	c.logger.Info("logical backup job suspended")

	return nil
}

// unsuspendLogicalBackupJob resumes the logical backup CronJob by setting spec.suspend=false.
// If the job was previously loaded but has been deleted externally, clears the cached reference.
// Returns nil if job is not loaded (no-op) or if job was not found (clears cache).
// Returns error only for actual failures (network errors, etc).
func (c *Cluster) unsuspendLogicalBackupJob() error {
	if c.LogicalBackupJob == nil {
		c.logger.Debug("logical backup job is not loaded, skipping unsuspend")
		return nil
	}

	// Check if job still exists (handles externally deleted jobs)
	_, err := c.KubeClient.CronJobsGetter.CronJobs(c.Namespace).Get(
		context.TODO(), c.getLogicalBackupJobName(), metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		c.logger.Info("logical backup job not found during unsuspend, clearing cached reference")
		c.LogicalBackupJob = nil
		return nil
	}
	if err != nil {
		return fmt.Errorf("could not get logical backup job: %w", err)
	}

	patchData := fmt.Sprintf(`{"spec":{"suspend":false}}`)
	cronJob, err := c.KubeClient.CronJobsGetter.CronJobs(c.Namespace).Patch(
		context.TODO(),
		c.getLogicalBackupJobName(),
		types.MergePatchType,
		[]byte(patchData),
		metav1.PatchOptions{},
		"",
	)
	if err != nil {
		return fmt.Errorf("could not resume logical backup job: %w", err)
	}
	c.LogicalBackupJob = cronJob
	c.logger.Info("logical backup job resumed")

	return nil
}

// getPoolerReplicas returns the current replica count for a pooler deployment.
// Returns 0 if pooler doesn't exist, hasn't been synced yet, or has nil Replicas.
func (c *Cluster) getPoolerReplicas(role PostgresRole) int32 {
	if c.ConnectionPooler == nil || c.ConnectionPooler[role] == nil ||
		c.ConnectionPooler[role].Deployment == nil ||
		c.ConnectionPooler[role].Deployment.Spec.Replicas == nil {
		return 0
	}
	return *c.ConnectionPooler[role].Deployment.Spec.Replicas
}

// scalePoolerDown scales all connection pooler deployments to 0 and stores their current
// replica counts in newSpec.Status.PreviousPoolerInstances.
// Should be called during hibernate initiation.
// Errors are returned immediately if a patch fails (partial state possible).
func (c *Cluster) scalePoolerDown(newSpec *acidv1.Postgresql) error {
	if c.ConnectionPooler == nil {
		return nil
	}

	for role := range c.ConnectionPooler {
		replicas := c.getPoolerReplicas(role)

		if newSpec.Status.PreviousPoolerInstances == nil {
			newSpec.Status.PreviousPoolerInstances = make(map[string]int32)
		}
		newSpec.Status.PreviousPoolerInstances[string(role)] = replicas

		if replicas > 0 {
			if err := c.patchPoolerReplicas(role, 0); err != nil {
				return err
			}
			c.logger.Infof("[lifecycle] pooler %s scaled to 0 (was %d)", role, replicas)
		}
	}
	return nil
}

// scalePoolerUp restores connection pooler deployments to their previous replica counts
// from newSpec.Status.PreviousPoolerInstances.
// Should be called during wake-up.
// Errors are returned immediately if a patch fails (partial state possible).
func (c *Cluster) scalePoolerUp(newSpec *acidv1.Postgresql) error {
	if newSpec.Status.PreviousPoolerInstances == nil {
		return nil
	}

	for roleStr, replicas := range newSpec.Status.PreviousPoolerInstances {
		role := PostgresRole(roleStr)

		if err := c.patchPoolerReplicas(role, replicas); err != nil {
			return err
		}
		c.logger.Infof("[lifecycle] pooler %s scaled to %d", role, replicas)
	}
	return nil
}

// patchPoolerReplicas patches a pooler deployment's replica count.
// If the deployment doesn't exist (not found), returns nil (no-op).
// Returns error for other failures (network errors, etc).
func (c *Cluster) patchPoolerReplicas(role PostgresRole, replicas int32) error {
	_, err := c.KubeClient.Deployments(c.Namespace).Get(
		context.TODO(), c.connectionPoolerName(role), metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("could not get pooler deployment for %s: %w", role, err)
	}

	patchData := fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas)
	_, err = c.KubeClient.Deployments(c.Namespace).Patch(
		context.TODO(), c.connectionPoolerName(role), types.MergePatchType, []byte(patchData), metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("could not patch pooler deployment %s replicas: %w", role, err)
	}
	return nil
}

