package cluster

import (
	acidv1 "github.com/zalando/postgres-operator/pkg/apis/acid.zalan.do/v1"
)

// manageHibernateState manages cluster hibernate/wake-up state transitions.
// Returns true if sync should continue, false if it should return early.
//
// This function handles the following state transitions:
//   - Running -> Stopping: When user sets lifecycle.phase = "stopped"
//   - Stopping -> Stopped: When StatefulSet replicas reach 0
//   - Stopped -> Updating: When user clears lifecycle.phase (wake-up)
//   - Updating -> Running: Normal sync continues, defer sets final status
func (c *Cluster) manageHibernateState(oldSpec acidv1.Postgresql, newSpec *acidv1.Postgresql) bool {

	// FIX B: Detect wake-up by comparing oldSpec status vs newSpec status
	// When Update() is called, it sets status=Updating before Sync() runs.
	// So we need to check if oldSpec.Status was Stopped and newSpec is Updating
	// with lifecycle cleared to properly detect wake-up.
	isWakingUp := oldSpec.Status.Stopped() &&
		newSpec.Status.PostgresClusterStatus == acidv1.ClusterStatusUpdating &&
		(newSpec.Spec.Lifecycle == nil || newSpec.Spec.Lifecycle.Phase != "stopped")

	// FIX C: Additional wake-up detection with simpler condition
	// If lifecycle was cleared and we have previousNumberOfInstances and numberOfInstances is 0
	isWakingUpSimple := newSpec.Spec.Lifecycle == nil || newSpec.Spec.Lifecycle.Phase != "stopped"
	hasPreviousInstances := newSpec.Status.PreviousNumberOfInstances > 0
	needsRestore := newSpec.Spec.NumberOfInstances == 0

	isWakingUp = isWakingUp || (isWakingUpSimple && hasPreviousInstances && needsRestore)

	// === INITIATE HIBERNATE: Running -> Stopping ===
	// Only initiate if not already stopping or stopped, and lifecycle.phase = "stopped"
	if newSpec.Spec.Lifecycle != nil &&
		newSpec.Spec.Lifecycle.Phase == "stopped" &&
		!newSpec.Status.Stopping() &&
		!newSpec.Status.Stopped() {

		newSpec.Status.PreviousNumberOfInstances = newSpec.Spec.NumberOfInstances
		newSpec.Status.PostgresClusterStatus = acidv1.ClusterStatusStopping
		newSpec.Spec.NumberOfInstances = 0
		c.logger.Infof("[lifecycle] cluster is going to hibernate, stored previous number of instances: %d",
			newSpec.Status.PreviousNumberOfInstances)
		return true
	}

	// === STOPPING -> STOPPED: Check actual StatefulSet replicas ===
	// Only transition to Stopped when StatefulSet replicas have actually reached 0
	if newSpec.Status.Stopping() {
		if c.Statefulset != nil && *c.Statefulset.Spec.Replicas == 0 {
			newSpec.Status.PostgresClusterStatus = acidv1.ClusterStatusStopped
			c.logger.Infof("[lifecycle] cluster has stopped, all pods are terminated")
		}
		return true
	}

	// === WAKE-UP: Stopped/Updating -> Running ===
	// Restore numberOfInstances from previousNumberOfInstances when waking up
	if newSpec.Status.Stopped() || isWakingUp {
		// Check if lifecycle.phase was cleared (user wants to wake up)
		if isWakingUp || newSpec.Spec.Lifecycle == nil || newSpec.Spec.Lifecycle.Phase != "stopped" {
			if newSpec.Status.PreviousNumberOfInstances > 0 {
				newSpec.Spec.NumberOfInstances = newSpec.Status.PreviousNumberOfInstances
				c.logger.Infof("[lifecycle] cluster is waking up, restoring number of instances: %d",
					newSpec.Status.PreviousNumberOfInstances)
			} else {
				c.logger.Warningf("[lifecycle] cluster is waking up but previousNumberOfInstances is 0, cannot restore")
			}
			newSpec.Status.PostgresClusterStatus = acidv1.ClusterStatusUpdating
			return true
		}
		// Still stopped and lifecycle.phase = "stopped", skip further sync
		return false
	}

	return true
}