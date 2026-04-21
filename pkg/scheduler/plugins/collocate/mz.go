package collocate

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

const (
	mzColocationGroup  = "node.linkedin.com/maintenance-zone"
	mzScoreArgumentKey = "mzScore"
)

type mz struct {
	mzNodes map[string][]*api.NodeInfo
	mzScore float64
}

func newMZColocation(ssn *framework.Session, args framework.Arguments) Colocation {
	plugin := &mz{
		mzNodes: make(map[string][]*api.NodeInfo),
		mzScore: 0.0,
	}
	for _, node := range ssn.Nodes {
		mz := node.Node.Labels[mzColocationGroup]
		if _, found := plugin.mzNodes[mz]; !found {
			plugin.mzNodes[mz] = make([]*api.NodeInfo, 0)
		}
		plugin.mzNodes[mz] = append(plugin.mzNodes[mz], node)
	}
	klog.V(3).Infof("Found %d MZs in the cluster", len(plugin.mzNodes))

	if args != nil {
		args.GetFloat64(&plugin.mzScore, mzScoreArgumentKey)
	}

	return plugin
}

/**
*
**/
func (mz *mz) BatchNodeOrder(candidate *api.TaskInfo, nodes []*api.NodeInfo, ssn *framework.Session, ranker *NodeGroupRanker) (map[string]float64, error) {
	nodeScores := make(map[string]float64)
	job := ssn.Jobs[candidate.Job]
	if job == nil {
		klog.Warningf("Skipping BatchNodeOrderFn for task %s because the Job is nil", candidate.Name)
		return nodeScores, nil
	}
	// We do not provide MZ colocation scores for non-gang jobs.
	// The length of nodes can be 0 in the Preempt action.
	if job.MinAvailable < 2 || len(nodes) == 0 {
		klog.V(3).Infof("Skipping BatchNodeOrderFn for task %s because either the Job is non-gang (minAvailable: %d) or there are no nodes (count: %d)", candidate.Name, ssn.Jobs[candidate.Job].MinAvailable, len(nodes))
		return nodeScores, nil
	}

	// If any pods in the Job are already allocated to a MZ and if the nodes provided are from that MZ, then score those nodes.
	// The rest of the nodes will not be given a score.
	currentMZs := mz.getMZJobRunningOn(ssn, job)
	// Identify if any of the Nodes that can run this task belong to the MZs that the Job is already running on.
	// If yes, assign scores to those nodes.
	mzFound := false
	for _, node := range nodes {
		if node.Node.Labels[mzColocationGroup] == "" {
			klog.Errorf("Node %s does not have the MZ label %s", node.Name, mzColocationGroup)
			continue
		}
		if _, ok := currentMZs[node.Node.Labels[mzColocationGroup]]; ok {
			// TODO make the score configurable
			nodeScores[node.Name] = mz.mzScore
			mzFound = true
		}
	}
	if mzFound {
		klog.V(3).Infof("Found MZs %v for task %s/%s in Job %s during BatchNodeOrder scoring for the MZ colocation plugin", currentMZs, candidate.Namespace, candidate.Name, job.Name)
		return nodeScores, nil
	}
	// The Nodes that can run this task do not belong to the MZs that the Job is already running on.
	// We have to identify the MZ that can fit the most number of Pending tasks from this Job.
	nodeGroups := mz.prepareMZGroups(nodes, ssn)
	// This is a defensive check. This should not happen ideally.
	if nodeGroups.Len() == 0 {
		klog.Warningf("%d MZs were found for the Job %s", nodeGroups.Len(), candidate.Job)
		return nodeScores, nil
	}
	// Identify the tasks in the Job that are yet to be scheduled and run matching only for these tasks.
	tasks := ssn.Jobs[candidate.Job].TaskStatusIndex[api.Pending]
	tasksCloned := make(map[api.TaskID]*api.TaskInfo)
	for _, task := range tasks {
		tasksCloned[task.UID] = task.Clone()
	}

	// Holds the list of NodePools that can schedule max number of tasks in a Job
	sortedMZs := ranker.rank(nodeGroups, tasksCloned, ssn)
	preferredMZ := sortedMZs[0]
	klog.Infof("selecting MZ: %s for job %s", preferredMZ.name, candidate.Job)
	for _, node := range nodes {
		if node.Node.Labels[mzColocationGroup] == preferredMZ.name {
			nodeScores[node.Name] = mz.mzScore
		}
	}
	return nodeScores, nil
}

/**
* Get all the MZs the Job is running on.
**/
func (mz *mz) getMZJobRunningOn(ssn *framework.Session, job *api.JobInfo) map[string]bool {
	currentMZs := make(map[string]bool)
	for _, task := range job.Tasks {
		if task.NodeName == "" {
			// Task is not scheduled on any node. We do not need to check the MZ for this task.
			continue
		}
		node := ssn.Nodes[task.NodeName]
		if node == nil {
			klog.Errorf("Node %s not found in the session cache. But the pod %s/%s is scheduled on the node", task.NodeName, task.Namespace, task.Name)
			continue
		}
		if node.Node.Labels[mzColocationGroup] == "" {
			klog.Errorf("Node %s does not have the MZ label %s", node.Name, mzColocationGroup)
			continue
		}
		// api.Allocated, api.Pipelined, api.Binding, api.Bound, api.Running are the task states that represent that the task is running on the Node or will run on the Node.
		if task.Status == api.Allocated || task.Status == api.Pipelined || task.Status == api.Binding || task.Status == api.Bound || task.Status == api.Running {
			currentMZs[node.Node.Labels[mzColocationGroup]] = true
		}
	}
	return currentMZs
}

/*
* We do not filter anything out here. This is because we are not restricting the tasks of a Job to a single MZ.
**/
func (mz *mz) Predicate(candidate *api.TaskInfo, node *api.NodeInfo) error {
	return nil
}

func (mz *mz) CloseSession(ssn *framework.Session) {
	mz.mzNodes = nil
	mz.mzScore = 0.0
}

func (mz *mz) TaskAllocated(event *framework.Event, ssn *framework.Session) {
	// No-op
}

func (mz *mz) TaskDeallocated(event *framework.Event, ssn *framework.Session) {
	// No-op
}

/**
* Iterate over the Nodes received in the BatchNodeOrderFn and identify the MZ of these Nodes.
* For each MZ identified, get all the Nodes that belong to that MZ and add them to the NodeGroupList.
**/
func (mz *mz) prepareMZGroups(nodes []*api.NodeInfo, _ *framework.Session) *NodeGroupList {
	nodeGroups := make(map[string][]*api.NodeInfo)
	// Identify the MZs from the list of Nodes that can run this task
	for _, node := range nodes {
		label := node.Node.Labels[mzColocationGroup]
		if _, found := nodeGroups[label]; !found {
			nodeGroups[label] = mz.mzNodes[label]
		}
	}

	nodeGroupList := make([]*NodeGroup, 0)
	for mz, nodesList := range nodeGroups {
		nodeGroupList = append(nodeGroupList, &NodeGroup{
			name:        mz,
			schedulable: int16(0),
			score:       0.0,
			resource:    nil,
			nodes:       nodesList,
		})
	}
	return &NodeGroupList{
		items:  nodeGroupList,
		lessFn: mzComparator,
	}
}

/**
* Sort the MZs based on the following order :
* 1. sum of scores for individual nodes in the MZ. Higher the number, better the MZ.
* 2. number of GPUs available on the MZ. Lower the number, better the MZ.
* 3. number of schedulable tasks on the MZ. Higher the number, better the MZ.
**/
func mzComparator(i, j *NodeGroup) bool {
	if i.score == j.score {
		if i.resource.availableScalarResource[v1.ResourceName(GPUResourceName)] == j.resource.availableScalarResource[v1.ResourceName(GPUResourceName)] {
			return i.schedulable > j.schedulable
		}
		return i.resource.availableScalarResource[v1.ResourceName(GPUResourceName)] < j.resource.availableScalarResource[v1.ResourceName(GPUResourceName)]
	}
	return i.score > j.score
}
