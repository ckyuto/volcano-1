/*
Pods submitted to the UQM scheduler requires quota approval from the UQM service.
The UQM service consistently monitors specific pods and grants quota approval based on availability.
All approved pods receive a "uqm.linkedin.com/quota-allocated" label.
This plugin examines the input Pod spec for the UQM label, allowing them to be assigned to a node by Volcano.
UQM pods are distinguishable by an annotation added by the client, such as the KingKong webhook.

UQM considers gang scheduling requirements when approving quota requests.
The UQM approves all necessary pods in a single action, contingent upon
the availability of sufficient quota for the entire gang. Therefore,
from the perspective of Volcano, the presence of the approved label signifies
readiness to allocate a node for the associated pod.

TODO
Note: This situation may create a potential issue where the cluster encounters difficulties
in binding pods to nodes, possibly due to fragmentation. Nevertheless, from UQM's perspective,
the overall capacity in the cluster appears to be adequate.
This is handled in uqm service by de-allocating the Pods, If it is not scheduled by volcano within
the defined period (eg: 10 mins).
*/

package uqm

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/actions/allocate"
	"volcano.sh/volcano/pkg/scheduler/actions/enqueue"
	"volcano.sh/volcano/pkg/scheduler/actions/preempt"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/metrics"
	"volcano.sh/volcano/pkg/scheduler/plugins/util"
)

var (
	uqmAcceptedPods = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: metrics.VolcanoSubSystemName,
			Name:      "uqm_plugin_uqm_pods_accepted",
			Help:      "The number of UQM Pods accepted by the uqm plugin",
		}, []string{"queue_name"},
	)
	nonUqmAcceptedPods = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: metrics.VolcanoSubSystemName,
			Name:      "uqm_plugin_nonuqm_pods_accepted",
			Help:      "The number of Non-UQM Pods accepted by the uqm plugin",
		}, []string{"queue_name"},
	)
	sessionLifeCycleTracker       = enqueue.New().Name()
	preemptableNodes              = []string{}
	preemptableTasks              = []*api.TaskInfo{}
	preemptableTaskStatuses       = []api.TaskStatus{api.Allocated, api.Pipelined, api.Binding, api.Bound, api.Running}
	taskUnderGuaranteedTimestamps = map[string]int64{}           // TaskUID -> parsed timestamp for UNDER_GUARANTEED pods
	jobEarliestTimestamps         = map[string]*sortedTaskList{} // JobUID -> sorted task list with nextIndex
)

// TaskTimestamp holds a task's UID and its under-guaranteed allocation timestamp
type TaskTimestamp struct {
	TaskUID   api.TaskID
	Timestamp int64
}

// sortedTaskList maintains a sorted list of tasks by timestamp with an index for efficient lookup
type sortedTaskList struct {
	tasks     []TaskTimestamp
	nextIndex int // Next index to start searching from (for lazy deletion)
}

const (
	// PluginName indicates name of volcano scheduler plugin.
	PluginName = "uqm"
	// quota-name label for branching uqm/non-uqm flow
	quotaNameLabelKey = "quota.linkedin.com/quota-name"
	// once the pod is allocated by uqm, same is updated using label with value "true"
	quotaAllocatedLabelKey     = "quota.linkedin.com/quota-allocated"
	quotaAllocatedAtLabelKey   = "quota.linkedin.com/quota-allocated-at"
	quotaPreemptableLabelKey   = "quota.linkedin.com/quota-preemptable"
	quotaPreemptableAtLabelKey = "quota.linkedin.com/quota-preemptable-at"
	quotaAllocatedFromLabel    = "quota.linkedin.com/quota-capacity-state-at-allocation"
	spotQuotaEligibleLabel     = "interruptible"
	spotQuotaAllocatedLabel    = "quota.linkedin.com/spot-allocated"
	spotCapacity               = "node.linkedin.com/spot-capacity"
	// Values for quota-capacity-state-at-allocation label
	underGuaranteedState = "UNDER_GUARANTEED"
	overGuaranteedState  = "OVER_GUARANTEED"
)

type uqmPlugin struct {
	// Arguments given for the plugin
	pluginArguments framework.Arguments
}

// New return proportion action
func New(arguments framework.Arguments) framework.Plugin {
	klog.V(3).Infof("registering " + PluginName + " plugin ...")
	return &uqmPlugin{
		pluginArguments: arguments,
	}
}

func (pp *uqmPlugin) Name() string {
	return PluginName
}

// compareTimestamps compares two int64 timestamp pointers.
// Returns: -1 if left has higher priority, 0 if equal, 1 if right has higher priority.
// Non-nil (UNDER_GUARANTEED) > nil (OVER_GUARANTEED or no timestamp).
// Within non-nil timestamps, earlier timestamp has higher priority.
func compareTimestamps(leftTimestamp, rightTimestamp *int64) int {
	if leftTimestamp != nil && rightTimestamp != nil {
		if *leftTimestamp < *rightTimestamp {
			return -1 // left allocated earlier, higher priority
		}
		if *leftTimestamp > *rightTimestamp {
			return 1 // right allocated earlier, higher priority
		}
		return 0 // Same timestamp
	} else if leftTimestamp != nil {
		// Only left has UNDER_GUARANTEED timestamp
		return -1
	} else if rightTimestamp != nil {
		// Only right has UNDER_GUARANTEED timestamp
		return 1
	}
	// Neither has UNDER_GUARANTEED timestamp
	return 0
}

// getJobEarliestTimestamp returns the earliest UNDER_GUARANTEED timestamp
// from the job's current pending tasks using the preprocessed sorted list.
// It uses lazy deletion: skips over tasks that are no longer pending and updates
// nextIndex to the position of the first valid pending task found.
// Returns nil if no UNDER_GUARANTEED pending tasks found.
func getJobEarliestTimestamp(job *api.JobInfo) *int64 {
	list, exists := jobEarliestTimestamps[string(job.UID)]
	if !exists || list.nextIndex >= len(list.tasks) {
		return nil
	}

	pendingTasks := job.TaskStatusIndex[api.Pending]

	// Start from nextIndex and find the first task that is still pending
	for i := list.nextIndex; i < len(list.tasks); i++ {
		tt := list.tasks[i]
		if _, stillPending := pendingTasks[tt.TaskUID]; stillPending {
			// Found a valid pending task, update nextIndex for next lookup
			list.nextIndex = i
			return &tt.Timestamp
		}
		// Task is no longer pending (already scheduled), skip it
	}

	// All tasks have been scheduled
	return nil
}

// formatTimestamp formats a timestamp pointer for logging.
// Returns the timestamp value as string, or "<nil>" if the pointer is nil.
func formatTimestamp(ts *int64) string {
	if ts == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%d", *ts)
}

func (pp *uqmPlugin) OnSessionOpen(ssn *framework.Session) {
	sessionLifeCycleTracker = enqueue.New().Name()
	ssn.AddAllocatableFn(pp.Name(), func(queue *api.QueueInfo, candidate *api.TaskInfo) bool {
		klog.V(4).Infof("Executing AllocatableFn for pod name: %s/%s from PodGoup : %s and quotaName : %s",
			candidate.Pod.Name, candidate.Pod.Namespace,
			candidate.Pod.GetAnnotations()[v1beta1.KubeGroupNameAnnotationKey],
			candidate.Pod.GetLabels()[quotaNameLabelKey])
		sessionLifeCycleTracker = allocate.New().Name()
		return isReadyForScheduling(candidate.Pod)
	})
	/*
		The JobStarvingFn is used to check if the job is starving for resources.
		This function is called in Preempt action. If the job is starving, then the job is considered to be a preemptor job.
		A Job is considered to be starving when there are tasks in the Job that are marked for scheduling
		and the number of tasks ready to be scheduled + scheduled on a node + succeeded > minAvailable.
		Without this function all the Pending tasks in the Job are considered to be ready to be scheduled.
	*/
	ssn.AddJobStarvingFns(pp.Name(), func(obj interface{}) bool {
		ji, ok := obj.(*api.JobInfo)
		if !ok {
			klog.Error("invalid object type for JobStarvingFn")
			return false
		}
		klog.V(4).Infof("Executing JobStarvingFn for job name: %s from PodGoup : %s",
			ji.Name, ji.PodGroup.ObjectMeta.Name)
		// This is an indication that the session has entered Preemption phase
		sessionLifeCycleTracker = preempt.New().Name()
		// Calculating the number of tasks that are either scheduled Or successfully finished.
		occupied := ji.WaitingTaskNum() + getNumberOfScheduledOrFinishedTasks(ji)
		/*
			If the number of tasks that are either scheduled Or finished + tasksReadyTobeScheduled
			is less than the minAvailable, then the job could not pass the gang semantics
			and we return false.
		*/
		readyToBeScheduled := getNumberOfTasksToBeScheduled(ji)
		klog.V(4).Infof("PodGroup : %s, readyToBeScheduled : %d, occupied : %d, minAvailable : %d", ji.PodGroup.ObjectMeta.Name, readyToBeScheduled, occupied, ji.MinAvailable)
		if readyToBeScheduled+occupied < ji.MinAvailable {
			klog.V(4).Infof("PodGroup : %s is not starving due to not meeting minAvailable requirement", ji.PodGroup.ObjectMeta.Name)
			return false
		}
		/*
			For gang-jobs that have occupied >= minAvailable or for non-gang jobs,
			we return true if there are any tasks that are ready to be scheduled.
		*/
		return readyToBeScheduled > 0
	})
	/*
		This function is only used during the Preemption action. This is No-Op during the allocate action.
		This PrePredicateFn is used to filter out the Pods that are not ready for scheduling.
		It checks the scheduling label on the Pod to determine that.
		Any pod that is not ready for scheduling will not be considered as a preemptor task.
		Without this function, all the tasks in the job are considered as preemptor tasks.
	*/
	ssn.AddPrePredicateFn(pp.Name(), func(task *api.TaskInfo) error {
		klog.V(4).Infof("Executing PrePredicate function for pod name: %s/%s from PodGoup : %s Action : %s quotaName : %s",
			task.Pod.Namespace, task.Pod.Name,
			task.Pod.GetAnnotations()[v1beta1.KubeGroupNameAnnotationKey],
			sessionLifeCycleTracker, task.Pod.GetLabels()[quotaNameLabelKey])
		if sessionLifeCycleTracker == preempt.New().Name() {
			if !isReadyForScheduling(task.Pod) {
				return fmt.Errorf("pod %s/%s from podgroup : %s is not ready for scheduling",
					task.Pod.Namespace, task.Pod.Name,
					task.Pod.GetAnnotations()[v1beta1.KubeGroupNameAnnotationKey])
			}
			klog.V(4).Infof("pod %s/%s from PodGroup : %s can be considered as a preemptor task",
				task.Pod.Namespace, task.Pod.Name,
				task.Pod.GetAnnotations()[v1beta1.KubeGroupNameAnnotationKey])
		}
		return nil
	})
	/*
		This function is only used during the Preemption action. This is No-Op during the allocate action.
		The Predicate function is used to determine if a preemptor task can be scheduled on this node.
		This function is used to filter out the nodes that do not have any tasks marked for preemption.
		Without this function, nodes without any preemptable tasks are considered for scheduling the preemptor task.
	*/
	ssn.AddPredicateFn(pp.Name(), func(candidate *api.TaskInfo, node *api.NodeInfo) error {
		klog.V(4).Infof("Executing Predicate function for pod : %s/%s from PodGoup : %s Action : %s on Node : %s",
			candidate.Pod.Namespace, candidate.Pod.Name, candidate.Pod.GetAnnotations()[v1beta1.KubeGroupNameAnnotationKey],
			sessionLifeCycleTracker, node.Name)

		_, spotQuotaRequested := candidate.Pod.Labels[spotQuotaEligibleLabel]
		if !spotQuotaRequested {
			// filter out spot capacity nodes
			if _, spotCapacity := node.Node.Labels[spotCapacity]; spotCapacity {
				return fmt.Errorf("spot-capacity node cannot be used for non spot quota eligible pod %s/%s", candidate.Namespace, candidate.Name)
			}
			// Non spot nodes should not have any errors returned.
		} else {
			// the pod is spot quota eligible, we need to check if quota is allocated from spot ?
			// Only pods that are quota allocated, will reach this step. UQM must add the label to indicate where the quota is allocated from.
			if quota, exists := candidate.Pod.Labels[spotQuotaAllocatedLabel]; exists && quota == "true" {
				// filter out non spot nodes
				if _, spotCapacity := node.Node.Labels[spotCapacity]; !spotCapacity {
					return fmt.Errorf("spot-capacity node cannot be used for non spot quota eligible pod %s/%s", candidate.Namespace, candidate.Name)
				}
			} else {
				// filter out spot nodes
				if _, spotCapacity := node.Node.Labels[spotCapacity]; spotCapacity {
					return fmt.Errorf("spot-capacity node cannot be used for non spot quota eligible pod %s/%s", candidate.Namespace, candidate.Name)
				}
			}
		}
		if sessionLifeCycleTracker == preempt.New().Name() {
			anyTaskMarkedForPreemption := false
			for _, task := range node.Tasks {
				if isMarkedForPreemption(task.Pod) {
					klog.V(3).Infof("pod %s/%s from podgroup %s is marked for preemption on node %s",
						task.Pod.Namespace, task.Pod.Name, task.Pod.Annotations[v1beta1.KubeGroupNameAnnotationKey], node.Name)
					anyTaskMarkedForPreemption = true
					break
				}
			}
			if !anyTaskMarkedForPreemption {
				return fmt.Errorf("no task marked for preemption on node %s", node.Name)
			}
		}
		return nil
	})
	/*
		This function is only called during the Preemption action.
		The PreemptableFn is used to determine the tasks that can be preempted by the preemptor task.
		This function is used to filter out the tasks that are not marked for preemption.
		Without this function, tasks on the node without preemption label are considered for preemption.
	*/
	ssn.AddPreemptableFn(pp.Name(), func(preemptor *api.TaskInfo, preemptees []*api.TaskInfo) ([]*api.TaskInfo, int) {
		klog.V(4).Infof("Executing PreemptableFn for preemptor pod : %s/%s from PodGoup : %s",
			preemptor.Pod.Namespace, preemptor.Pod.Name,
			preemptor.Pod.GetAnnotations()[v1beta1.KubeGroupNameAnnotationKey])
		victims := make([]*api.TaskInfo, 0)
		for _, task := range preemptees {
			if isMarkedForPreemption(task.Pod) {
				klog.V(3).Infof("found a preemptable task <%s/%s> marked at %s (podGroup: %s, node: %s) for the preemptor task <%s/%s> (podGroup: %s)",
					task.Pod.Namespace, task.Pod.Name,
					task.Pod.GetLabels()[quotaPreemptableAtLabelKey],
					task.Pod.GetAnnotations()[v1beta1.KubeGroupNameAnnotationKey], task.Pod.Spec.NodeName,
					preemptor.Pod.Namespace, preemptor.Pod.Name,
					preemptor.Pod.GetAnnotations()[v1beta1.KubeGroupNameAnnotationKey])
				victims = append(victims, task)
			}
		}
		return victims, util.Permit
	})
	/*
		This function is only called during the Preemption action.
		The PreemptableNodesFn is used to get a list of nodes that have atleast one task marked for preemption.
		Without this function, all the nodes are considered for scheduling the preemptor task.
	*/
	ssn.AddPreemptableNodesFn(pp.Name(), func() []string {
		return preemptableNodes
	})
	ssn.AddPreemptableTasksFn(pp.Name(), func() []*api.TaskInfo {
		return preemptableTasks
	})
	/*
		JobOrderFn to prioritize jobs with UNDER_GUARANTEED pods over OVER_GUARANTEED pods.
		Within UNDER_GUARANTEED jobs, earlier allocated-at timestamp has higher priority.
	*/
	ssn.AddJobOrderFn(pp.Name(), func(l, r interface{}) int {
		lv, ok := l.(*api.JobInfo)
		if !ok {
			klog.Errorf("UQM JobOrderFn: left argument is not *api.JobInfo, type: %T", l)
			return 0
		}
		rv, ok := r.(*api.JobInfo)
		if !ok {
			klog.Errorf("UQM JobOrderFn: right argument is not *api.JobInfo, type: %T", r)
			return 0
		}

		// Get earliest timestamp from current pending tasks using preprocessed map
		leftEarliestTimestamp := getJobEarliestTimestamp(lv)
		rightEarliestTimestamp := getJobEarliestTimestamp(rv)

		klog.V(4).Infof("UQM JobOrderFn: comparing Job <%v/%v> (pending-tasks: %d, min-under-timestamp: %s) with Job <%v/%v> (pending-tasks: %d, min-under-timestamp: %s)",
			lv.Namespace, lv.Name, len(lv.TaskStatusIndex[api.Pending]), formatTimestamp(leftEarliestTimestamp),
			rv.Namespace, rv.Name, len(rv.TaskStatusIndex[api.Pending]), formatTimestamp(rightEarliestTimestamp))

		return compareTimestamps(leftEarliestTimestamp, rightEarliestTimestamp)
	})
	/*
		TaskOrderFn to prioritize tasks with UNDER_GUARANTEED capacity state over OVER_GUARANTEED.
		Within UNDER_GUARANTEED tasks, earlier allocated-at timestamp has higher priority.
	*/
	ssn.AddTaskOrderFn(pp.Name(), func(l interface{}, r interface{}) int {
		lv, ok := l.(*api.TaskInfo)
		if !ok {
			klog.Errorf("UQM TaskOrderFn: left argument is not *api.TaskInfo, type: %T", l)
			return 0
		}
		rv, ok := r.(*api.TaskInfo)
		if !ok {
			klog.Errorf("UQM TaskOrderFn: right argument is not *api.TaskInfo, type: %T", r)
			return 0
		}

		// Get timestamp from preprocessed map
		var leftTimestamp, rightTimestamp *int64
		if ts, exists := taskUnderGuaranteedTimestamps[string(lv.Pod.UID)]; exists {
			leftTimestamp = &ts
		}
		if ts, exists := taskUnderGuaranteedTimestamps[string(rv.Pod.UID)]; exists {
			rightTimestamp = &ts
		}

		klog.V(4).Infof("UQM TaskOrderFn: <%v/%v> (under-timestamp: %s); <%v/%v> (under-timestamp: %s)",
			lv.Namespace, lv.Name, formatTimestamp(leftTimestamp),
			rv.Namespace, rv.Name, formatTimestamp(rightTimestamp))

		return compareTimestamps(leftTimestamp, rightTimestamp)
	})
	preProcessPreemptableData(ssn)
	preProcessUnderGuaranteedTimestamps(ssn)
}

// Returns the number of tasks that are either scheduled Or finished successfully.
func getNumberOfScheduledOrFinishedTasks(ji *api.JobInfo) int32 {
	occupied := 0
	occupied += len(ji.TaskStatusIndex[api.Bound])
	occupied += len(ji.TaskStatusIndex[api.Binding])
	occupied += len(ji.TaskStatusIndex[api.Running])
	occupied += len(ji.TaskStatusIndex[api.Allocated])
	occupied += len(ji.TaskStatusIndex[api.Succeeded])

	return int32(occupied)
}

func getNumberOfTasksToBeScheduled(ji *api.JobInfo) int32 {
	ready := 0
	if tasks, found := ji.TaskStatusIndex[api.Pending]; found {
		for _, task := range tasks {
			if isReadyForScheduling(task.Pod) {
				ready++
			}
		}
	}
	return int32(ready)
}

func (pp *uqmPlugin) OnSessionClose(ssn *framework.Session) {
	sessionLifeCycleTracker = "closed"
}

// This function checks for UQM quota approval status for uqm pods and is not preemptable and return true
// if available otherwise returns true for non-uqm flow pods.
func isReadyForScheduling(pod *v1.Pod) bool {
	labels := pod.GetLabels()
	//Check for uqm-quota quota label, if yes they should route it to uqm-quota for scheduling
	quotaName, exists := labels[quotaNameLabelKey]
	if !exists {
		//If no label found, it is a regular flow.
		klog.V(3).Infof("UQM semantics not applicable to regular pod [%s/%s] from PodGroup : %s",
			pod.GetNamespace(), pod.GetName(), pod.GetAnnotations()[v1beta1.KubeGroupNameAnnotationKey])
		updateAcceptedNonUqmPods(quotaName)
		return true
	}

	//If the job is routed to uqm, then check for provisioned label, if found schedule them
	value, exists := labels[quotaAllocatedLabelKey]
	if !exists || value != "true" {
		klog.V(3).Infof("UQM plugin deemed task <%s/%s> as NOT allocatable. quotaName : %s, podGroup : %s",
			pod.GetNamespace(), pod.GetName(), quotaName, pod.GetAnnotations()[v1beta1.KubeGroupNameAnnotationKey])
		return false
	}

	// if the job is marked as preemptable, don't schedule them
	if isMarkedForPreemption(pod) {
		klog.V(3).Infof("UQM plugin deemed task <%s/%s> as NOT allocatable since pod is preemptable. quotaName : %s, podGroup : %s",
			pod.GetNamespace(), pod.GetName(), quotaName, pod.GetAnnotations()[v1beta1.KubeGroupNameAnnotationKey])
		return false
	}

	updateAcceptedUqmPods(quotaName)
	klog.V(3).Infof("UQM plugin deemed task <%s/%s> as allocatable at %s. quota: %s, podGroup : %s",
		pod.GetNamespace(), pod.GetName(), labels[quotaAllocatedAtLabelKey], quotaName,
		pod.GetAnnotations()[v1beta1.KubeGroupNameAnnotationKey])
	return true
}

// This function initializes the PreemptableNodes for the session and populates the map with the nodes if any.
// It also creates a list of preemptable tasks for the session.
func preProcessPreemptableData(ssn *framework.Session) {
	preemptableNodes = []string{}
	preemptableTasks = []*api.TaskInfo{}
	for _, ts := range preemptableTaskStatuses {
		for job := range ssn.Jobs {
			for _, task := range ssn.Jobs[job].TaskStatusIndex[ts] {
				if isMarkedForPreemption(task.Pod) {
					preemptableNodes = append(preemptableNodes, task.NodeName)
					preemptableTasks = append(preemptableTasks, task)
				}
			}
		}
	}
	klog.V(3).Infof("uqm plugin: populated %d preemptable nodes and %d preemtable tasks",
		len(preemptableNodes), len(preemptableTasks))
}

// parseTimestampLabel parses a timestamp string from pod labels.
// Returns the parsed timestamp and true if successful, or 0 and false if parsing fails.
// Timestamps are expected to be Unix timestamps (seconds since epoch) as strings.
func parseTimestampLabel(timestampStr string) (int64, bool) {
	if timestampStr == "" {
		return 0, false
	}

	timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		return 0, false
	}

	return timestamp, true
}

// preProcessUnderGuaranteedTimestamps parses and caches UNDER_GUARANTEED timestamps
// for all tasks in the session to avoid repeated string parsing during job/task ordering.
// This function is called once per session in OnSessionOpen and builds:
// 1. taskUnderGuaranteedTimestamps: map from TaskUID to parsed timestamp
// 2. jobEarliestTimestamps: map from JobUID to sorted list of pending tasks with timestamps
func preProcessUnderGuaranteedTimestamps(ssn *framework.Session) {
	// Step 1: Build task-level timestamp map
	taskUnderGuaranteedTimestamps = make(map[string]int64)

	for _, job := range ssn.Jobs {
		for _, tasks := range job.TaskStatusIndex {
			for _, task := range tasks {
				// Check if this task has UNDER_GUARANTEED state
				if state, exists := task.Pod.Labels[quotaAllocatedFromLabel]; exists && state == underGuaranteedState {
					// Parse and cache the timestamp
					if allocatedAt, exists := task.Pod.Labels[quotaAllocatedAtLabelKey]; exists {
						if timestamp, ok := parseTimestampLabel(allocatedAt); ok {
							taskUnderGuaranteedTimestamps[string(task.Pod.UID)] = timestamp
						} else {
							klog.Warningf("Failed to parse allocated-at timestamp '%s' for pod %s/%s",
								allocatedAt, task.Pod.Namespace, task.Pod.Name)
						}
					}
				}
			}
		}
	}

	klog.V(3).Infof("uqm plugin: preprocessed %d UNDER_GUARANTEED task timestamps", len(taskUnderGuaranteedTimestamps))

	// Step 2: Build job-level sorted task lists for efficient lookup
	jobEarliestTimestamps = make(map[string]*sortedTaskList)

	for _, job := range ssn.Jobs {
		// Collect all under-guaranteed pending tasks for this job
		var taskTimestamps []TaskTimestamp

		for _, task := range job.TaskStatusIndex[api.Pending] {
			taskUID := string(task.Pod.UID)
			if ts, exists := taskUnderGuaranteedTimestamps[taskUID]; exists {
				taskTimestamps = append(taskTimestamps, TaskTimestamp{
					TaskUID:   task.UID,
					Timestamp: ts,
				})
			}
		}

		// Only create sorted list if there are under-guaranteed tasks
		if len(taskTimestamps) > 0 {
			// Sort by timestamp (earliest first)
			sort.Slice(taskTimestamps, func(i, j int) bool {
				return taskTimestamps[i].Timestamp < taskTimestamps[j].Timestamp
			})

			jobEarliestTimestamps[string(job.UID)] = &sortedTaskList{
				tasks:     taskTimestamps,
				nextIndex: 0,
			}

			klog.V(4).Infof("uqm plugin: job %s/%s has %d under-guaranteed pending tasks, earliest timestamp: %d",
				job.Namespace, job.Name, len(taskTimestamps), taskTimestamps[0].Timestamp)
		}
	}

	klog.V(3).Infof("uqm plugin: preprocessed %d jobs with under-guaranteed pending tasks", len(jobEarliestTimestamps))
}

func isMarkedForPreemption(pod *v1.Pod) bool {
	return pod.Labels[quotaPreemptableLabelKey] == "true"
}

func updateAcceptedUqmPods(quotaName string) {
	uqmAcceptedPods.WithLabelValues(quotaName).Inc()
}

func updateAcceptedNonUqmPods(quotaName string) {
	nonUqmAcceptedPods.WithLabelValues(quotaName).Inc()
}
