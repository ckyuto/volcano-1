package collocate

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

const (
	ibColocationGroup = "node.linkedin.com/pool"
)

type infiniband struct {
	jobNodeGroupMap map[api.JobID]*NodeGroup
}

func newIBColocation(ssn *framework.Session) Colocation {
	return &infiniband{
		jobNodeGroupMap: mapJobToNodeGroup(ssn),
	}
}

/**
* Filter out the Nodes that are from a different NodePool than the one assigned to the Job.
 */
func (ib *infiniband) Predicate(candidate *api.TaskInfo, node *api.NodeInfo) error {
	nodeGroup, colocated := ib.jobNodeGroupMap[candidate.Job]
	if candidate.Pod.Annotations[collocateAnnotation] == "required" && colocated && ibMigratedTo(node.Node.Labels[ibColocationGroup]) != nodeGroup.name {
		return fmt.Errorf("cannot schedule job %s on Node %s (node group : %s) as some job pods are already running on node group %s ", candidate.Job, node.Name, ibMigratedTo(node.Node.Labels[ibColocationGroup]), nodeGroup.name)
	}
	return nil
}

func (ib *infiniband) BatchNodeOrder(candidate *api.TaskInfo, nodes []*api.NodeInfo, ssn *framework.Session, ranker *NodeGroupRanker) (map[string]float64, error) {
	nodeScores := make(map[string]float64)

	// The BatchNodeOrderFn is also called when we mimic the scheduling of the tasks in the Job.
	// When that happens, we should not rerun the BatchNodeScores. Else we will be stuck in a loop.
	// We should also not run the BatchNodeScores for the tasks that do not require colocation.
	if candidate.Pod.Annotations[collocateAnnotation] != "required" || len(nodes) == 0 {
		klog.V(4).Infof("Skipping BatchNodeOrderFn for task %s because either colocation is not required (%s) or there are no nodes (%d)", candidate.Name, candidate.Pod.Annotations[collocateAnnotation], len(nodes))
		return nodeScores, nil
	}

	// If the Job is already assigned to a NodePool, then we give a 100 score to the nodes of that NodePool.
	if nodeGroup, colocated := ib.jobNodeGroupMap[candidate.Job]; colocated {
		for _, node := range nodes {
			if ibMigratedTo(node.Node.Labels[ibColocationGroup]) == nodeGroup.name {
				nodeScores[node.Name] = 100
			}
		}
		return nodeScores, nil
	}
	// Start scoring NodeGroups
	klog.V(3).Info("Finding NodePool for the Job ", candidate.Job)
	// TODO : Add a metric to track the time taken to find the NodePool for this Job.
	// Job is not assigned to a NodePool. We have to identify the NodePool
	nodeGroups := prepareNodeGroups(nodes, ssn)
	// This is a defensive check. This should not happen ideally.
	if nodeGroups.Len() == 0 {
		klog.Errorf("%d NodeGroups were found for the Job %s", nodeGroups.Len(), candidate.Job)
		return nodeScores, nil
	}
	// Identify the tasks in the Job that are yet to be scheduled and run matching only for these tasks.
	tasks := ssn.Jobs[candidate.Job].TaskStatusIndex[api.Pending]
	tasksCloned := make(map[api.TaskID]*api.TaskInfo)
	for _, task := range tasks {
		if task.Pod.Annotations[collocateAnnotation] != "required" {
			continue
		}
		tasksCloned[task.UID] = task.Clone()
	}
	// Holds the list of NodePools that can schedule max number of tasks in a Job
	sortedNodeGroups := ranker.rank(nodeGroups, tasksCloned, ssn)
	preferredNodeGroup := sortedNodeGroups[0]
	klog.Infof("selecting nodegroup: %s for job %s", preferredNodeGroup.name, candidate.Job)
	for _, node := range nodes {
		if ibMigratedTo(node.Node.Labels[ibColocationGroup]) == preferredNodeGroup.name {
			nodeScores[node.Name] = 100
		}
	}
	return nodeScores, nil
}

func mapJobToNodeGroup(ssn *framework.Session) map[api.JobID]*NodeGroup {
	jobNodeGroupMap := make(map[api.JobID]*NodeGroup)
	for _, jobInfo := range ssn.Jobs {
		for _, taskInfo := range jobInfo.Tasks {
			if taskInfo.Pod.Annotations[collocateAnnotation] != "required" {
				continue
			}
			if !api.AllocatedStatus(taskInfo.Status) && taskInfo.Status != api.Pipelined {
				continue
			}
			nodeInfo, found := ssn.Nodes[taskInfo.NodeName]
			if !found {
				klog.Errorf("Skipping nodeGroup mapping for job %s/%s as the Node %s the pod is scheduled on is not found in scheduler cache", taskInfo.Namespace, taskInfo.Name, taskInfo.NodeName)
				continue
			}
			if nodeGroup, found := jobNodeGroupMap[jobInfo.UID]; !found {
				// In future when we have more colocation dimensions, we need to store all the KV for that job in this map.
				jobNodeGroupMap[jobInfo.UID] = &NodeGroup{
					name: ibMigratedTo(nodeInfo.Node.Labels[ibColocationGroup]),
				}
			} else if nodeGroup.name != ibMigratedTo(nodeInfo.Node.Labels[ibColocationGroup]) {
				// Ideally this should not happen.
				// We can either override the existing NodePool or continue using it.
				klog.Errorf("Job %s/%s is assigned to 2 different NodePools %s and %s", jobInfo.Namespace, jobInfo.Name, jobNodeGroupMap[jobInfo.UID].name, ibMigratedTo(nodeInfo.Node.Labels[ibColocationGroup]))
			}
		}
	}
	return jobNodeGroupMap
}

func prepareNodeGroups(nodes []*api.NodeInfo, ssn *framework.Session) *NodeGroupList {
	nodeGroups := make(map[string][]*api.NodeInfo)
	// Identify the nodepool names from the list of Nodes that have passed the predicates for this task
	for _, node := range nodes {
		// converting the nodepool name to the new nodepool name when the node is being migrated.
		label := ibMigratedTo(node.Node.Labels[ibColocationGroup])
		if _, found := nodeGroups[label]; !found {
			nodeGroups[label] = make([]*api.NodeInfo, 0)
		}
	}
	// Group all the Nodes based on the NodePool name. In this step we do not limit ourselves from the Nodes that have passed the predicates.
	// This is because we are trying to match nodes with other tasks from the Job that are yet to be scheduled.
	for _, node := range ssn.Nodes {
		label := ibMigratedTo(node.Node.Labels[ibColocationGroup])
		if _, found := nodeGroups[label]; found {
			nodeGroups[label] = append(nodeGroups[label], node)
		}
	}
	nodeGroupList := make([]*NodeGroup, 0)
	for nodeGroupName, nodesList := range nodeGroups {
		nodeGroupList = append(nodeGroupList, &NodeGroup{
			name:        nodeGroupName,
			schedulable: int16(0),
			score:       0.0,
			resource:    nil,
			nodes:       nodesList,
		})
	}
	return &NodeGroupList{
		items:  nodeGroupList,
		lessFn: less,
	}
}

/**
* Sort the NodePools based on the following order :
* 1. number of schedulable tasks on the NodePools. Higher the number, better the NodePools.
* 2. number of GPUs available on the NodePools. Lower the number, better the NodePools.
* 3. sum of scores for individual nodes in the NodePools. Higher the number, better the NodePools.
**/
func less(i, j *NodeGroup) bool {
	if i.schedulable == j.schedulable {
		if i.resource.availableScalarResource[v1.ResourceName(GPUResourceName)] == j.resource.availableScalarResource[v1.ResourceName(GPUResourceName)] {
			return i.score > j.score
		}
		return i.resource.availableScalarResource[v1.ResourceName(GPUResourceName)] < j.resource.availableScalarResource[v1.ResourceName(GPUResourceName)]
	}
	return i.schedulable > j.schedulable
}

func (ib *infiniband) TaskAllocated(event *framework.Event, ssn *framework.Session) {
	nodeGroup := &NodeGroup{
		name: ibMigratedTo(ssn.Nodes[event.Task.NodeName].Node.Labels[ibColocationGroup]),
	}
	if event.Task.Pod.Annotations[collocateAnnotation] != "required" {
		// For Pods that do not require colocation we should not store the pool information as they can be scheduled on any nodepool.
		return
	}
	if group, found := ib.jobNodeGroupMap[event.Task.Job]; !found {
		ib.jobNodeGroupMap[event.Task.Job] = nodeGroup
	} else {
		// Do nothing
		if group.name != nodeGroup.name {
			// This should not happen. But if it does, we should investigate it.
			// TODO: Add a metric here.
			klog.Errorf("Job %s is assigned to NodePool %s and later scheduled on %s", event.Task.Job, group.name, nodeGroup.name)
		}
	}
}

func (ib *infiniband) TaskDeallocated(event *framework.Event, ssn *framework.Session) {
	// We do not need to do anything here.
}

func (ib *infiniband) CloseSession(ssn *framework.Session) {
	ib.jobNodeGroupMap = nil
}

/**
* This function returns the new IB NodePool the hosts are migrated to.
* If the nodepool is not being migrated, it returns the same nodepool.
**/
func ibMigratedTo(from string) string {
	if to, ok := IBNodePoolMigrations[from]; ok {
		return to
	}
	return from
}
