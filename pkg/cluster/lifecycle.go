package cluster

import (
	"context"
	"fmt"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

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
	// When Update() is called, it sets status=Updating before Sync() runs.
	// So we need to check if oldSpec.Status was Stopped and newSpec is Updating
	// with lifecycle cleared to properly detect wake-up.
	isWakingUp := oldSpec.Status.Stopped() &&
		newSpec.Status.PostgresClusterStatus == acidv1.ClusterStatusUpdating &&
		(newSpec.Spec.Lifecycle == nil || newSpec.Spec.Lifecycle.Phase != "stopped")

	// If lifecycle was cleared and we have previousNumberOfInstances and numberOfInstances is 0
	isWakingUpSimple := newSpec.Spec.Lifecycle == nil || newSpec.Spec.Lifecycle.Phase != "stopped"
	hasPreviousInstances := newSpec.Status.PreviousNumberOfInstances > 0
	needsRestore := newSpec.Spec.NumberOfInstances == 0

	// double verification of waking up
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
			c.logger.Info("[lifecycle] cluster has stopped, all pods are terminated")
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

// getPoolerReplicas returns the current replica count for a pooler role.
// Returns 0 if pooler doesn't exist or hasn't been synced yet.
func (c *Cluster) getPoolerReplicas(role PostgresRole) int32 {
	if c.ConnectionPooler == nil || c.ConnectionPooler[role] == nil ||
		c.ConnectionPooler[role].Deployment == nil ||
		c.ConnectionPooler[role].Deployment.Spec.Replicas == nil {
		return 0
	}
	return *c.ConnectionPooler[role].Deployment.Spec.Replicas
}

// scalePoolerDown scales pooler deployments to 0 and stores current replica count.
// Should be called during hibernate initiation.
func (c *Cluster) scalePoolerDown(newSpec *acidv1.Postgresql) error {
	if c.ConnectionPooler == nil {
		return nil
	}

	for role := range c.ConnectionPooler {
		replicas := c.getPoolerReplicas(role)

		// Store current replicas in status
		if newSpec.Status.PreviousPoolerInstances == nil {
			newSpec.Status.PreviousPoolerInstances = make(map[string]int32)
		}
		newSpec.Status.PreviousPoolerInstances[string(role)] = replicas

		// Scale to 0 if currently non-zero
		if replicas > 0 {
			if err := c.patchPoolerReplicas(role, 0); err != nil {
				return err
			}
			c.logger.Infof("[lifecycle] pooler %s scaled to 0 (was %d)", role, replicas)
		}
	}
	return nil
}

// scalePoolerUp restores pooler deployments to previous replica counts.
// Should be called during wake-up.
func (c *Cluster) scalePoolerUp(newSpec *acidv1.Postgresql) error {
	if newSpec.Status.PreviousPoolerInstances == nil {
		return nil
	}

	for roleStr, replicas := range newSpec.Status.PreviousPoolerInstances {
		role := PostgresRole(roleStr)

		// Scale to stored value (could be 0)
		if err := c.patchPoolerReplicas(role, replicas); err != nil {
			return err
		}
		c.logger.Infof("[lifecycle] pooler %s scaled to %d", role, replicas)
	}
	return nil
}

// patchPoolerReplicas patches the pooler deployment replica count.
func (c *Cluster) patchPoolerReplicas(role PostgresRole, replicas int32) error {
	// Check if deployment exists first
	_, err := c.KubeClient.Deployments(c.Namespace).Get(
		context.TODO(), c.connectionPoolerName(role), metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil // Pooler doesn't exist, no-op
		}
		return fmt.Errorf("could not get pooler deployment for %s: %w", role, err)
	}

	// Patch with merge patch
	patchData := fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas)
	_, err = c.KubeClient.Deployments(c.Namespace).Patch(
		context.TODO(), c.connectionPoolerName(role), types.MergePatchType, []byte(patchData), metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("could not patch pooler deployment %s replicas: %w", role, err)
	}
	return nil
}

