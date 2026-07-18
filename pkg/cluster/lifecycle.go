package cluster

import (
	"context"
	"fmt"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	acidv1 "github.com/zalando/postgres-operator/pkg/apis/acid.zalan.do/v1"
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
	wantsStopped := newSpecLifecycle != nil && newSpecLifecycle.Phase == "stopped"

	// The cluster was in the middle of hibernating (already scaled down,
	// old replica count saved) when the spec changed its mind and no
	// longer wants it stopped.
	if !wantsStopped && newSpecPreviousNumberOfInstances > 0 && newSpecNumberOfInstances == 0 {
		return LifecycleActionWakeUp
	}

	// Already stopped and the spec no longer asks to stay stopped.
	if currentStatus.Stopped() && !wantsStopped {
		return LifecycleActionWakeUp
	}

	if wantsStopped && !currentStatus.Stopping() && !currentStatus.Stopped() {
		return LifecycleActionHibernate
	}
	return LifecycleActionNone
}

// handleHibernateAndWakeUp detects a hibernate/wake-up transition and asks
// syncStateLocked to mutate + persist the spec+status and run the full sync
// body.
// For hibernate it then waits inline for pods to terminate and persist Stopped.
// For wake-up the sync's existing defer transitions Updating → Running
// once resources exist.
//
// Returns (handled bool, err error):
//   - (true, nil)   lifecycle transition completed; Update() should return early
//   - (false, nil)  no lifecycle transition; Update() proceeds normally
//   - (true, err)   transition attempted but failed; caller returns the error
func (c *Cluster) handleHibernateAndWakeUp(newSpec *acidv1.Postgresql) (bool, error) {
	action := detectLifecycleTransition(
		&c.Status,
		newSpec.Spec.Lifecycle,
		newSpec.Spec.NumberOfInstances,
		newSpec.Status.PreviousNumberOfInstances,
	)

	if action == LifecycleActionNone {
		return false, nil
	}

	// syncStateLocked → prepareLifecycleTransition: detects the transition,
	// mutates the spec via initiateHibernate/initiateWakeUp, and immediately
	// persists spec+status.
	if err := c.syncStateLocked(newSpec); err != nil {
		return true, fmt.Errorf("could not sync after lifecycle transition: %w", err)
	}

	// Hibernate: wait inline for pods to actually terminate and persist Stopped.
	// Doing this here (outside syncStateLocked) means a timeout returns an error
	// without triggering the defer's SyncFailed status — the operator controller
	// requeues and the next Sync (or this same Update on retry) resumes the wait.
	if action == LifecycleActionHibernate {
		if err := c.completeStoppingTransition(newSpec); err != nil {
			return true, fmt.Errorf("hibernate failed: %w", err)
		}
	}

	return true, nil
}

// prepareLifecycleTransition inspects newSpec for a lifecycle transition
// On detection it mutates the in-memory spec via initiateHibernate/initiateWakeUp
// and immediately persists BOTH spec and status to the K8s API so the new status
// (Stopping/Updating) is visible in same reconciliation pass — without waiting for
// syncStateLocked's defer.
// Uses oldSpec for detection so it works correctly when handleHibernateAndWakeUp
// has already mutated newSpec before calling syncStateLocked.
//
// Returns:
//   - (true,  nil): proceed with the rest of syncStateLocked (no transition,
//     or transition was persisted)
//   - (false, nil): skip sync — cluster is Stopped with no transition requested
//   - (false, err): error persisting the spec or status; caller propagates to
//     controller (next Update will retry the whole transition)
func (c *Cluster) prepareLifecycleTransition(newSpec **acidv1.Postgresql, oldSpec acidv1.Postgresql) (bool, error) {
	spec := *newSpec
	action := detectLifecycleTransition(
		&oldSpec.Status,
		spec.Spec.Lifecycle,
		spec.Spec.NumberOfInstances,
		spec.Status.PreviousNumberOfInstances,
	)

	// Stopped cluster with lifecycle=stopped: no reconciliation work.
	if action == LifecycleActionNone && c.Status.Stopped() {
		return false, nil
	}

	switch action {
	case LifecycleActionHibernate:
		c.initiateHibernate(spec)
	case LifecycleActionWakeUp:
		c.initiateWakeUp(spec)
	}

	if action == LifecycleActionHibernate || action == LifecycleActionWakeUp {
		// Persist spec first (writes numInst/lifecycle + advances rv to N+1),
		// then write the status subresource (advances rv to N+2).
		pgUpdated, err := c.KubeClient.UpdatePostgresCR(c.clusterName(), spec)
		if err != nil {
			return false, fmt.Errorf("could not update spec for lifecycle action: %w", err)
		}
		// UpdatePostgresCR returns the CR with a fresh resourceVersion but the
		// status subresource may be empty/stale. Re-apply the lifecycle status
		// fields before writing the status.
		pgUpdated.Status.PreviousNumberOfInstances = spec.Status.PreviousNumberOfInstances
		pgUpdated.Status.PreviousPoolerInstances = spec.Status.PreviousPoolerInstances
		pgUpdated.Status.PostgresClusterStatus = spec.Status.PostgresClusterStatus

		pgUpdated, err = c.KubeClient.SetPostgresCRDStatus(c.clusterName(), pgUpdated)
		if err != nil {
			return false, fmt.Errorf("could not set status for lifecycle action: %w", err)
		}
		c.setSpec(pgUpdated)
	}

	return true, nil
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
//   - Restoring numberOfInstances from PreviousNumberOfInstances (if > 0)
//   - Scaling up connection pooler deployments (consumes PreviousPoolerInstances)
//   - Resuming logical backup CronJob
//   - Setting status to Updating
//   - Clearing PreviousNumberOfInstances / PreviousPoolerInstances so they don't
//     linger in the status subresource after the wake-up completes. The next
//     hibernate overwrites them with fresh values.
//
// If PreviousNumberOfInstances is 0, logs a warning but still sets status to
// Updating (operator-restart catch-up may have already cleared it).
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

	newSpec.Status.PreviousNumberOfInstances = 0
	newSpec.Status.PreviousPoolerInstances = nil
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

// completeStoppingTransition waits for the StatefulSet pods to actually terminate
// and transitions the cluster status from Stopping to Stopped. On timeout, returns
// an error so the caller can propagate it to the controller which will retry.
func (c *Cluster) completeStoppingTransition(newSpec *acidv1.Postgresql) error {
	if err := c.waitStatefulsetPodsGone(); err != nil {
		return fmt.Errorf("could not wait for pods to terminate: %w", err)
	}

	// Re-fetch the latest resourceVersion before writing Stopped to stop 409 and drift.
	latest, err := c.KubeClient.Postgresqls(c.clusterNamespace()).Get(
		context.TODO(), c.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("could not refresh postgresql before persisting Stopped: %w", err)
	}
	newSpec.ResourceVersion = latest.ResourceVersion

	newSpec.Status.PostgresClusterStatus = acidv1.ClusterStatusStopped
	c.setSpec(newSpec)
	if _, err := c.persistStoppingCompletedTransition(newSpec); err != nil {
		return err
	}
	c.logger.Info("[lifecycle] cluster has stopped")
	return nil
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

	patchData := `{"spec":{"suspend":true}}`
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

	patchData := `{"spec":{"suspend":false}}`
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
