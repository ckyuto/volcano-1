package collocate

import (
	"sort"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/util"
)

/*
* This contains the list of NodeGroups that are being considered for scheduling the tasks in the Job
* and the compare function that will be used in ranking the NodeGroups.
 */
type NodeGroupList struct {
	items  []*NodeGroup
	lessFn func(i, j *NodeGroup) bool
}

/*
* NodeGroupRanker simulates scheduling for the tasks in the Job on a given NodeGroup.
* The results of the scheduling of a task on a Node are cached in "taskNodeScoreMap". This cache helps other plugins during the scheduling simulation.
 */
type NodeGroupRanker struct {
	taskNodeScoreMap map[api.TaskID]map[string]float64
	fakeScoring      bool
}

/*
* This stores the resource usage of a NodeGroup for a Job.
 */
type nodeGroupResource struct {
	avialableCPU            float64
	availableMem            float64
	availableScalarResource map[v1.ResourceName]float64
}

/*
* This holds the results of the scheduling simulation on a NodeGroup for a Job.
 */
type NodeGroup struct {
	name        string
	schedulable int16
	score       float64
	resource    *nodeGroupResource
	nodes       []*api.NodeInfo
}

func newNodeGroupRanker() *NodeGroupRanker {
	return &NodeGroupRanker{
		taskNodeScoreMap: make(map[api.TaskID]map[string]float64),
		fakeScoring:      false,
	}
}

// Start of sort interface implementation
func (ngList *NodeGroupList) Len() int {
	return len(ngList.items)
}

func (ngList *NodeGroupList) Less(i, j int) bool {
	return ngList.lessFn(ngList.items[i], ngList.items[j])
}

func (ngList *NodeGroupList) Swap(i, j int) {
	ngList.items[i], ngList.items[j] = ngList.items[j], ngList.items[i]
}

// End of sort interface implementation

/*
* This function ranks the NodePools based on the number of tasks that can be scheduled on the NodePool and the score of the NodePool.
 */
func (ranker *NodeGroupRanker) rank(nodeGroups *NodeGroupList, tasks map[api.TaskID]*api.TaskInfo, ssn *framework.Session) []*NodeGroup {
	// TODO work more on the initial checks
	if nodeGroups.Len() == 0 || len(tasks) == 0 {
		return nodeGroups.items
	}

	for _, nodeGroup := range nodeGroups.items {
		// cloning the nodes so that we can subtract the task resources from the node resources after allocation.
		// This is to ensure that the orginal node resources are not modified.
		nodesCloned := make([]*api.NodeInfo, len(nodeGroup.nodes))
		for i, node := range nodeGroup.nodes {
			clonedNode := node.Clone()
			nodesCloned[i] = clonedNode
		}
		klog.V(3).Infof("Calculating schedulable task count on NodePool %s", nodeGroup.name)
		// klog.Infof("Running simulation for tasks %v on nodes %v", tasks, nodesCloned)
		schedulableTasks, nodeGroupScore, availableResource := ranker.simulateScheduling(ssn, tasks, nodesCloned)
		klog.V(3).Infof("schedulable tasks: %d, NodeGroup score: %f and resource: %+v for NodeGroup %s", schedulableTasks, nodeGroupScore, availableResource, nodeGroup.name)
		// Record the score and schedulable tasks for this NodePool.
		nodeGroup.schedulable = schedulableTasks
		nodeGroup.score = nodeGroupScore
		nodeGroup.resource = availableResource
	}
	// Sort the NodePools based on the lessFn configured by the colocation plugins.
	sort.Sort(nodeGroups)
	return nodeGroups.items
}

/**
* This function simulates the scheduling of the tasks in the Job on the NodePools.
* It returns the number of tasks that can be scheduled on the NodePool and the score of the NodePool.
 */
func (ranker *NodeGroupRanker) simulateScheduling(ssn *framework.Session, tasks map[api.TaskID]*api.TaskInfo, nodes []*api.NodeInfo) (int16, float64, *nodeGroupResource) {
	ph := util.NewPredicateHelper()
	predicateFn := func(task *api.TaskInfo, node *api.NodeInfo) error {
		// Check for Resource Predicate
		if !task.InitResreq.LessEqual(node.FutureIdle(), api.Zero) {
			return api.NewFitError(task, node, api.NodeResourceFitFailed)
		}

		return ssn.PredicateFn(task, node)
	}

	var totalIdle *api.Resource = api.EmptyResource()
	var totalAllocatable *api.Resource = api.EmptyResource()
	for _, node := range nodes {
		totalIdle = totalIdle.Add(node.Idle)
		totalAllocatable = totalAllocatable.Add(node.Allocatable)
	}

	schedulableTasks := int16(0)
	nodeGroupScore := float64(0.0)
	for _, task := range tasks {
		queue := ssn.Queues[ssn.Jobs[task.Job].Queue]
		if !ssn.Allocatable(queue, task) || ssn.PrePredicateFn(task) != nil {
			continue
		}
		predicateNodes, _ := ph.PredicateNodes(task, nodes, predicateFn, false, nil)
		var bestNode *api.NodeInfo
		bestScore := 0.0
		switch {
		case len(predicateNodes) == 0: // Skip task if no candidate nodes were found.
			continue
		case len(predicateNodes) >= 1: // Use the single node satisfying the predicate.
			ranker.fakeScoring = true
			nodeScores := util.PrioritizeNodes(task, predicateNodes, ssn.BatchNodeOrderFn, ssn.NodeOrderMapFn, ssn.NodeOrderReduceFn)
			ranker.fakeScoring = false
			bestNode = ssn.BestNodeFn(task, nodeScores)
			if bestNode == nil {
				bestNode, _ = util.SelectBestNodeAndScore(nodeScores)
			}
			bestScore = ranker.getBestScore(nodeScores)
		}
		klog.V(3).Infof("Best Node for task %s is %s from node group: %s", task.UID, bestNode.Name, bestNode.Node.Labels[ibColocationGroup])
		if err := bestNode.AddTask(task); err != nil {
			klog.Warningf("Failed to add task <%s/%s> to node <%s> with err: %v",
				task.Namespace, task.Name, bestNode.Name, err)
			continue
		}
		// During AddTask call nodeName is set on the Task. I am unsetting it as this is a simulation.
		task.NodeName = ""
		// task has been assigned to a node in the nodepool
		schedulableTasks++
		nodeGroupScore += bestScore
		if task.InitResreq.LessEqual(totalIdle, api.Zero) {
			totalIdle = totalIdle.Sub(task.InitResreq)
		} else {
			klog.Errorf("Task %s/%s is scheduled on node %s but the nodegroup does not have the required idle resources", task.Namespace, task.Name, bestNode.Name)
		}
	}
	scalarResource := make(map[v1.ResourceName]float64)
	for rName, rQuant := range totalIdle.ScalarResources {
		scalarResource[rName] = rQuant
	}
	availableResource := &nodeGroupResource{
		avialableCPU:            totalIdle.MilliCPU,
		availableMem:            totalIdle.Memory,
		availableScalarResource: scalarResource,
	}

	return schedulableTasks, nodeGroupScore, availableResource
}

func (ranker *NodeGroupRanker) getBestScore(nodeScores map[float64][]*api.NodeInfo) float64 {
	maxScore := -1.0
	for score := range nodeScores {
		if score > maxScore {
			maxScore = score
		}
	}
	return maxScore
}
