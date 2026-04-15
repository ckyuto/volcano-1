/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package uqm

import (
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	scheduling "volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/cache"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/util/assert"
)

func TestPod_Readiness_ForScheduling(t *testing.T) {
	pod := buildPod("c1", "p1", "", fmt.Sprintf("%v-%v", "c1", "p1"), v1.PodPending, buildResourceList("5000m", "50G"),
		nil, make(map[string]string), make(map[string]string))

	//1.a Validate non-uqm flow
	ready := isReadyForScheduling(pod)
	if !ready {
		t.Errorf("expected pod ready for scheduling")
	}

	//1.b Validate non-uqm flow
	pod.Labels = make(map[string]string)
	ready = isReadyForScheduling(pod)
	if !ready {
		t.Errorf("expected pod ready for scheduling")
	}

	//2.a Validate uqm flow, quota name is updated but quota not allocated yet
	pod.Labels[quotaNameLabelKey] = "test"
	ready = isReadyForScheduling(pod)
	if ready {
		t.Errorf("expected pod not ready for scheduling, quota is not allocated")
	}

	//2.b Validate uqm flow, quota name is updated but quota not allocated yet
	pod.Labels[quotaAllocatedLabelKey] = "false"
	ready = isReadyForScheduling(pod)
	if ready {
		t.Errorf("expected pod not ready for scheduling, quota is not allocated")
	}

	//2.c Validate uqm flow, quota name is updated and quota is also allocated
	pod.Labels[quotaAllocatedLabelKey] = "true"
	ready = isReadyForScheduling(pod)
	if !ready {
		t.Errorf("expected pod ready for scheduling, quota is allocated")
	}

	// 3 a. validate uqm flow, when job is marked as preemptable
	pod.Labels[quotaPreemptableLabelKey] = "true"
	ready = isReadyForScheduling(pod)
	if ready {
		t.Errorf("expected pod not ready for scheduling, job is marked for preemptable")
	}
}

func buildPod(ns, n, nodeName, uid string,
	p v1.PodPhase, req v1.ResourceList,
	owner []metav1.OwnerReference, labels, annotations map[string]string) *v1.Pod {

	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:             types.UID(uid),
			Name:            n,
			Namespace:       ns,
			OwnerReferences: owner,
			Labels:          labels,
			Annotations:     annotations,
		},
		Status: v1.PodStatus{
			Phase: p,
		},
		Spec: v1.PodSpec{
			NodeName: nodeName,
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: req,
					},
				},
			},
		},
	}
}

func buildPodGroup(name string) *api.PodGroup {
	return &api.PodGroup{
		PodGroup: scheduling.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:   name,
				Labels: make(map[string]string),
			},
			Spec: scheduling.PodGroupSpec{
				Queue: "default",
			},
		},
	}
}

func buildResourceList(cpu string, memory string) v1.ResourceList {
	return v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse(cpu),
		v1.ResourceMemory: resource.MustParse(memory),
	}
}

func Test_JobStarving(t *testing.T) {
	uqmPlugin := &uqmPlugin{}
	enabledJobStarving := true
	pluginOption := conf.PluginOption{
		Name:               PluginName,
		EnabledJobStarving: &enabledJobStarving,
	}
	schedulerCache := cache.NewDefaultMockSchedulerCache("test-scheduler")
	ssn := framework.OpenSession(schedulerCache, []conf.Tier{
		{
			Plugins: []conf.PluginOption{pluginOption},
		},
	},
		[]conf.Configuration{
			{
				Name: "preempt",
			},
		},
	)
	uqmPlugin.OnSessionOpen(ssn)
	podAnnotations := make(map[string]string)
	podUID := "10e62f6e-c708-4208-9ca8-df148d9daa42"
	podGroupName := fmt.Sprintf("podgroup-%v", podUID)
	podGroup := buildPodGroup(podGroupName)
	podAnnotations[v1beta1.KubeGroupNameAnnotationKey] = podGroupName
	namespace := "default"
	jobId := fmt.Sprintf("%v/%v", namespace, podGroupName)

	podLabelsQuotaEnabled := make(map[string]string)
	podLabelsQuotaEnabled[quotaAllocatedLabelKey] = "true"
	podLabelsQuotaEnabled[quotaNameLabelKey] = "test"
	pendingQuotaAllocatedPod := buildPod(namespace, "test-pod", "", podUID, v1.PodPending,
		buildResourceList("5000m", "50G"), nil, podLabelsQuotaEnabled, podAnnotations)
	job1 := api.NewJobInfo(api.JobID(jobId), api.NewTaskInfo(pendingQuotaAllocatedPod))
	job1.PodGroup = podGroup
	job1.Name = podGroupName

	podLabelsQuotaNotEnabled := make(map[string]string)
	podLabelsQuotaNotEnabled[quotaNameLabelKey] = "test"
	pendingQuotaNotAllocatedPod := buildPod(namespace, "test-pod", "", podUID, v1.PodPending,
		buildResourceList("5000m", "50G"), nil, podLabelsQuotaNotEnabled, podAnnotations)
	job2 := api.NewJobInfo(api.JobID(jobId), api.NewTaskInfo(pendingQuotaNotAllocatedPod))
	job2.PodGroup = podGroup
	job2.Name = podGroupName

	podLabelsQuotaNotManaged := make(map[string]string)
	pendingQuotaNotManagedPod := buildPod(namespace, "test-pod", "", podUID, v1.PodPending,
		buildResourceList("5000m", "50G"), nil, podLabelsQuotaNotManaged, podAnnotations)
	job3 := api.NewJobInfo(api.JobID(jobId), api.NewTaskInfo(pendingQuotaNotManagedPod))
	job3.PodGroup = podGroup
	job3.Name = podGroupName

	job4 := api.NewJobInfo(api.JobID(jobId), api.NewTaskInfo(pendingQuotaNotAllocatedPod),
		api.NewTaskInfo(pendingQuotaAllocatedPod))
	job4.MinAvailable = 2
	job4.PodGroup = podGroup
	job4.Name = podGroupName

	job5 := api.NewJobInfo(api.JobID(jobId), api.NewTaskInfo(pendingQuotaNotAllocatedPod),
		api.NewTaskInfo(pendingQuotaAllocatedPod))
	job5.MinAvailable = 1
	job5.PodGroup = podGroup
	job5.Name = podGroupName

	type args struct {
		job *api.JobInfo
	}
	tests := []struct {
		name           string
		args           args
		expect         bool
		failureMessage string
	}{
		{
			name: "quota_allocated",
			args: args{
				job: job1,
			},
			expect:         true,
			failureMessage: "expected job to be starving",
		},
		{
			name: "quota_not_allocated",
			args: args{
				job: job2,
			},
			expect:         false,
			failureMessage: "expected job to not be starving",
		},
		{
			name: "quota_not_managed",
			args: args{
				job: job3,
			},
			expect:         true,
			failureMessage: "expected job to be starving",
		},
		{
			name: "gang_job_min_not_available",
			args: args{
				job: job4,
			},
			expect:         false,
			failureMessage: "expected job to not be starving",
		},
		{
			name: "gang_job_min_available",
			args: args{
				job: job5,
			},
			expect:         true,
			failureMessage: "expected job to be starving",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isJobStarving := ssn.JobStarving(tt.args.job)
			assert.Assert(isJobStarving == tt.expect, tt.failureMessage)
		})
	}
}

func Test_PrePredicate_Allocate(t *testing.T) {
	uqmPlugin := &uqmPlugin{}
	enabledPredicate := true
	pluginOption := conf.PluginOption{
		Name:             PluginName,
		EnabledPredicate: &enabledPredicate,
	}
	schedulerCache := cache.NewDefaultMockSchedulerCache("test-scheduler")
	ssn := framework.OpenSession(schedulerCache, []conf.Tier{
		{
			Plugins: []conf.PluginOption{pluginOption},
		},
	},
		[]conf.Configuration{
			{
				Name: "allocate",
			},
		},
	)
	uqmPlugin.OnSessionOpen(ssn)
	podAnnotations := make(map[string]string)
	podUID := "10e62f6e-c708-4208-9ca8-df148d9daa42"
	podGroupName := fmt.Sprintf("podgroup-%v", podUID)
	podAnnotations[v1beta1.KubeGroupNameAnnotationKey] = podGroupName
	namespace := "default"

	podLabelsQuotaEnabled := make(map[string]string)
	podLabelsQuotaEnabled[quotaAllocatedLabelKey] = "true"
	podLabelsQuotaEnabled[quotaNameLabelKey] = "test"
	pendingQuotaAllocatedPod := buildPod(namespace, "test-pod", "", podUID, v1.PodPending,
		buildResourceList("5000m", "50G"), nil, podLabelsQuotaEnabled, podAnnotations)

	err := ssn.PrePredicateFn(api.NewTaskInfo(pendingQuotaAllocatedPod))
	assert.Assert(err == nil, "expected task to pass pre-predicate")
}

func Test_PrePredicate_Preempt(t *testing.T) {
	podAnnotations := make(map[string]string)
	podUID := "10e62f6e-c708-4208-9ca8-df148d9daa42"
	podGroupName := fmt.Sprintf("podgroup-%v", podUID)
	podGroup := buildPodGroup(podGroupName)
	podAnnotations[v1beta1.KubeGroupNameAnnotationKey] = podGroupName
	namespace := "default"
	jobId := fmt.Sprintf("%v/%v", namespace, podGroupName)

	podLabelsQuotaEnabled := make(map[string]string)
	podLabelsQuotaEnabled[quotaAllocatedLabelKey] = "true"
	podLabelsQuotaEnabled[quotaNameLabelKey] = "test"
	pendingQuotaAllocatedPod := buildPod(namespace, "test-pod", "", podUID, v1.PodPending,
		buildResourceList("5000m", "50G"), nil, podLabelsQuotaEnabled, podAnnotations)

	podLabelsQuotaNotEnabled := make(map[string]string)
	podLabelsQuotaNotEnabled[quotaNameLabelKey] = "test"
	pendingQuotaNotAllocatedPod := buildPod(namespace, "test-pod", "", podUID, v1.PodPending,
		buildResourceList("5000m", "50G"), nil, podLabelsQuotaNotEnabled, podAnnotations)

	podLabelsQuotaNotManaged := make(map[string]string)
	pendingQuotaNotManagedPod := buildPod(namespace, "test-pod", "", podUID, v1.PodPending,
		buildResourceList("5000m", "50G"), nil, podLabelsQuotaNotManaged, podAnnotations)

	type args struct {
		pod *v1.Pod
	}

	tests := []struct {
		name           string
		args           args
		shouldFail     bool
		failureMessage string
	}{
		{
			name: "quota_allocated",
			args: args{
				pod: pendingQuotaAllocatedPod,
			},
			shouldFail:     false,
			failureMessage: "expected task to pass pre-predicate",
		},
		{
			name: "quota_not_allocated",
			args: args{
				pod: pendingQuotaNotAllocatedPod,
			},
			shouldFail:     true,
			failureMessage: "expected task to fail pre-predicate",
		},
		{
			name: "quota_not_managed",
			args: args{
				pod: pendingQuotaNotManagedPod,
			},
			shouldFail:     false,
			failureMessage: "expected task to fail pre-predicate",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uqmPlugin := &uqmPlugin{}
			enabledPredicate := true
			enabledJobStarving := true
			pluginOption := conf.PluginOption{
				Name:               PluginName,
				EnabledJobStarving: &enabledJobStarving,
				EnabledPredicate:   &enabledPredicate,
			}
			schedulerCache := cache.NewDefaultMockSchedulerCache("test-scheduler")
			ssn := framework.OpenSession(schedulerCache, []conf.Tier{
				{
					Plugins: []conf.PluginOption{pluginOption},
				},
			},
				[]conf.Configuration{
					{
						Name: "preempt",
					},
				},
			)
			uqmPlugin.OnSessionOpen(ssn)
			ti := api.NewTaskInfo(tt.args.pod)
			job := api.NewJobInfo(api.JobID(jobId), ti)
			job.PodGroup = podGroup
			job.Name = podGroupName
			ssn.JobStarving(job)
			err := ssn.PrePredicateFn(ti)
			if tt.shouldFail {
				assert.Assert(err != nil, tt.failureMessage)
			} else {
				assert.Assert(err == nil, tt.failureMessage)
			}
		})
	}
}

func Test_Predicate_Allocate(t *testing.T) {
	podAnnotations := make(map[string]string)
	podUID := "10e62f6e-c708-4208-9ca8-df148d9daa42"
	podGroupName := fmt.Sprintf("podgroup-%v", podUID)
	podAnnotations[v1beta1.KubeGroupNameAnnotationKey] = podGroupName
	namespace := "default"

	podLabelsQuotaEnabled := make(map[string]string)
	podLabelsQuotaEnabled[quotaAllocatedLabelKey] = "true"
	podLabelsQuotaEnabled[quotaNameLabelKey] = "test"
	pendingQuotaAllocatedPod := buildPod(namespace, "test-pod", "", podUID, v1.PodPending,
		buildResourceList("5000m", "50G"), nil, podLabelsQuotaEnabled, podAnnotations)

	uqmPlugin := &uqmPlugin{}
	enabledPredicate := true
	pluginOption := conf.PluginOption{
		Name:             PluginName,
		EnabledPredicate: &enabledPredicate,
	}
	schedulerCache := cache.NewDefaultMockSchedulerCache("test-scheduler")
	schedulerCache.Jobs[api.JobID(fmt.Sprintf("%s/%s", namespace, podGroupName))] = &api.JobInfo{
		UID:      api.JobID(fmt.Sprintf("%s/%s", namespace, podGroupName)),
		PodGroup: buildPodGroup(podGroupName),
		Queue:    api.QueueID("default"),
		Budget: &api.DisruptionBudget{
			MinAvailable:  "0",
			MaxUnavailable: "10",
		},
	}
	qi := api.NewQueueInfo(buildQueue())
	schedulerCache.Queues[qi.UID] = qi
	ssn := framework.OpenSession(schedulerCache, []conf.Tier{
		{
			Plugins: []conf.PluginOption{pluginOption},
		},
	},
		[]conf.Configuration{
			{
				Name: "allocate",
			},
		},
	)
	uqmPlugin.OnSessionOpen(ssn)
	ti := api.NewTaskInfo(pendingQuotaAllocatedPod)
	nonSpotNode := buildNode()
	err := ssn.PredicateFn(ti, &api.NodeInfo{
		Node: nonSpotNode,
	})
	assert.Assert(err == nil, "expected task to pass predicate")

	// Create a node with spot capacity label
	spotNode := buildNode()
	spotNode.Labels[spotCapacity] = "true"
	err = ssn.PredicateFn(ti, &api.NodeInfo{
		Node: spotNode,
	})
	assert.Assert(err != nil, "task fails predicate since the node is reserved for spot jobs")
}

func Test_Predicate_Spot_Quota_Allocate(t *testing.T) {
	podAnnotations := make(map[string]string)
	podUID := "10e62f6e-c708-4208-9ca8-df148d9daa42"
	podGroupName := fmt.Sprintf("podgroup-%v", podUID)
	podAnnotations[v1beta1.KubeGroupNameAnnotationKey] = podGroupName
	namespace := "default"

	uqmPlugin := &uqmPlugin{}
	enabledPredicate := true
	pluginOption := conf.PluginOption{
		Name:             PluginName,
		EnabledPredicate: &enabledPredicate,
	}
	schedulerCache := cache.NewDefaultMockSchedulerCache("test-scheduler")

	spotQuotaPG := buildPodGroup(podGroupName)
	schedulerCache.Jobs[api.JobID(fmt.Sprintf("%s/%s", namespace, podGroupName))] = &api.JobInfo{
		UID:      api.JobID(fmt.Sprintf("%s/%s", namespace, podGroupName)),
		PodGroup: spotQuotaPG,
		Queue:    api.QueueID("default"),
		Budget: &api.DisruptionBudget{
			MinAvailable:  "0",
			MaxUnavailable: "10",
		},
	}
	qi := api.NewQueueInfo(buildQueue())
	schedulerCache.Queues[qi.UID] = qi
	ssn := framework.OpenSession(schedulerCache, []conf.Tier{
		{
			Plugins: []conf.PluginOption{pluginOption},
		},
	},
		[]conf.Configuration{
			{
				Name: "allocate",
			},
		},
	)
	uqmPlugin.OnSessionOpen(ssn)

	podSpotQuotaEnabled := make(map[string]string)
	podSpotQuotaEnabled[quotaAllocatedLabelKey] = "true"
	podSpotQuotaEnabled[quotaNameLabelKey] = "test"
	podSpotQuotaEnabled[spotQuotaEligibleLabel] = "preferred"
	podSpotQuotaEnabled[spotQuotaAllocatedLabel] = "true"
	spotQuotaEligiblePod := buildPod(namespace, "test-pod", "", podUID, v1.PodPending,
		buildResourceList("5000m", "50G"), nil, podSpotQuotaEnabled, podAnnotations)
	ti := api.NewTaskInfo(spotQuotaEligiblePod)

	// Create a node with spot capacity label
	spotNode := buildNode()
	spotNode.Labels[spotCapacity] = "true"

	nonSpotNode := buildNode()

	err := ssn.PredicateFn(ti, &api.NodeInfo{
		Node: nonSpotNode,
	})
	assert.Assert(err != nil, "task fails predicate since the node is non-spot capacity and the job is allocated quota from spot")

	err = ssn.PredicateFn(ti, &api.NodeInfo{
		Node: spotNode,
	})
	assert.Assert(err == nil, "task succeeds predicate since the node is spot capacity and the job is allocated quota from spot")
}

func Test_Predicate_Non_Spot_Quota_Allocate(t *testing.T) {
	podAnnotations := make(map[string]string)
	podUID := "10e62f6e-c708-4208-9ca8-df148d9daa42"
	podGroupName := fmt.Sprintf("podgroup-%v", podUID)
	podAnnotations[v1beta1.KubeGroupNameAnnotationKey] = podGroupName
	namespace := "default"

	uqmPlugin := &uqmPlugin{}
	enabledPredicate := true
	pluginOption := conf.PluginOption{
		Name:             PluginName,
		EnabledPredicate: &enabledPredicate,
	}
	schedulerCache := cache.NewDefaultMockSchedulerCache("test-scheduler")

	spotQuotaPG := buildPodGroup(podGroupName)
	schedulerCache.Jobs[api.JobID(fmt.Sprintf("%s/%s", namespace, podGroupName))] = &api.JobInfo{
		UID:      api.JobID(fmt.Sprintf("%s/%s", namespace, podGroupName)),
		PodGroup: spotQuotaPG,
		Queue:    api.QueueID("default"),
		Budget: &api.DisruptionBudget{
			MinAvailable:  "0",
			MaxUnavailable: "10",
		},
	}
	qi := api.NewQueueInfo(buildQueue())
	schedulerCache.Queues[qi.UID] = qi
	ssn := framework.OpenSession(schedulerCache, []conf.Tier{
		{
			Plugins: []conf.PluginOption{pluginOption},
		},
	},
		[]conf.Configuration{
			{
				Name: "allocate",
			},
		},
	)
	uqmPlugin.OnSessionOpen(ssn)

	podSpotQuotaEnabled := make(map[string]string)
	podSpotQuotaEnabled[quotaAllocatedLabelKey] = "true"
	podSpotQuotaEnabled[quotaNameLabelKey] = "test"
	podSpotQuotaEnabled[spotQuotaEligibleLabel] = "preferred"
	podSpotQuotaEnabled[spotQuotaAllocatedLabel] = "false"
	spotQuotaEligiblePod := buildPod(namespace, "test-pod", "", podUID, v1.PodPending,
		buildResourceList("5000m", "50G"), nil, podSpotQuotaEnabled, podAnnotations)
	ti := api.NewTaskInfo(spotQuotaEligiblePod)

	// Create a node with spot capacity label
	spotNode := buildNode()
	spotNode.Labels[spotCapacity] = "true"

	nonSpotNode := buildNode()

	err := ssn.PredicateFn(ti, &api.NodeInfo{
		Node: spotNode,
	})
	assert.Assert(err != nil, "task fails predicate since the node is spot capacity and the job is allocated quota from non-spot")

	err = ssn.PredicateFn(ti, &api.NodeInfo{
		Node: nonSpotNode,
	})
	assert.Assert(err == nil, "task succeeds predicate since the node is non-spot capacity and the job is allocated quota from non-spot")
}

func Test_Predicate_Preempt(t *testing.T) {
	podAnnotations := make(map[string]string)
	podUID := "10e62f6e-c708-4208-9ca8-df148d9daa42"
	podGroupName := fmt.Sprintf("podgroup-%v", podUID)
	podAnnotations[v1beta1.KubeGroupNameAnnotationKey] = podGroupName
	namespace := "default"
	jobId := fmt.Sprintf("%v/%v", namespace, podGroupName)

	podLabelsQuotaEnabled := make(map[string]string)
	podLabelsQuotaEnabled[quotaAllocatedLabelKey] = "true"
	podLabelsQuotaEnabled[quotaNameLabelKey] = "test"
	pendingQuotaAllocatedPod := buildPod(namespace, "test-pod", "", podUID, v1.PodPending,
		buildResourceList("5000m", "50G"), nil, podLabelsQuotaEnabled, podAnnotations)

	uqmPlugin := &uqmPlugin{}
	enabledPredicate := true
	enabledJobStarving := true
	pluginOption := conf.PluginOption{
		Name:               PluginName,
		EnabledPredicate:   &enabledPredicate,
		EnabledJobStarving: &enabledJobStarving,
	}
	schedulerCache := cache.NewDefaultMockSchedulerCache("test-scheduler")
	schedulerCache.Jobs[api.JobID(fmt.Sprintf("%s/%s", namespace, podGroupName))] = &api.JobInfo{
		UID:      api.JobID(fmt.Sprintf("%s/%s", namespace, podGroupName)),
		PodGroup: buildPodGroup(podGroupName),
		Queue:    api.QueueID("default"),
		Budget: &api.DisruptionBudget{
			MinAvailable:  "0",
			MaxUnavailable: "10",
		},
	}
	qi := api.NewQueueInfo(buildQueue())
	schedulerCache.Queues[qi.UID] = qi
	ssn := framework.OpenSession(schedulerCache, []conf.Tier{
		{
			Plugins: []conf.PluginOption{pluginOption},
		},
	},
		[]conf.Configuration{
			{
				Name: "preempt",
			},
		},
	)
	uqmPlugin.OnSessionOpen(ssn)
	preemptorTask := api.NewTaskInfo(pendingQuotaAllocatedPod)
	job := api.NewJobInfo(api.JobID(jobId), preemptorTask)
	job.PodGroup = buildPodGroup(podGroupName)
	job.Name = podGroupName
	ssn.JobStarving(job)

	nodeWithPreemptableTask := api.NewNodeInfo(buildNode())
	podLabelsPreemptionMarked := make(map[string]string)
	podLabelsPreemptionMarked[quotaPreemptableLabelKey] = "true"
	podLabelsPreemptionMarked[quotaNameLabelKey] = "test"
	preempteePod := buildPod(namespace, "test-pod", "node1", podUID, v1.PodRunning,
		buildResourceList("5000m", "50G"), nil, podLabelsPreemptionMarked, podAnnotations)
	nodeWithPreemptableTask.AddTask(api.NewTaskInfo(preempteePod))

	nodeWithoutPreemptableTask := api.NewNodeInfo(buildNode())
	podLabelsWithNoPreemption := make(map[string]string)
	podLabelsWithNoPreemption[quotaNameLabelKey] = "test"
	podLabelsWithNoPreemption[quotaAllocatedLabelKey] = "true"
	runningPod := buildPod(namespace, "test-pod", "node1", podUID, v1.PodRunning,
		buildResourceList("5000m", "50G"), nil, podLabelsWithNoPreemption, podAnnotations)
	nodeWithoutPreemptableTask.AddTask(api.NewTaskInfo(runningPod))

	type args struct {
		node *api.NodeInfo
	}

	tests := []struct {
		name           string
		args           args
		shouldFail     bool
		failureMessage string
	}{
		{
			name: "node_with_preemptable_pod",
			args: args{
				node: nodeWithPreemptableTask,
			},
			shouldFail:     false,
			failureMessage: "expected task to pass predicate",
		},
		{
			name: "node_without_preemptable_pod",
			args: args{
				node: nodeWithoutPreemptableTask,
			},
			shouldFail:     true,
			failureMessage: "expected task to fail predicate",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ssn.PredicateFn(preemptorTask, tt.args.node)
			if tt.shouldFail {
				assert.Assert(err != nil, tt.failureMessage)
			} else {
				assert.Assert(err == nil, tt.failureMessage)
			}
		})
	}
}

func buildNode() *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node1",
			Labels: make(map[string]string),
		},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("10000m"),
				v1.ResourceMemory: resource.MustParse("100G"),
			},
			Capacity: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("10000m"),
				v1.ResourceMemory: resource.MustParse("100G"),
			},
		},
	}
}

func buildQueue() *scheduling.Queue {
	return &scheduling.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name: "default",
		},
	}
}

func Test_Preemptable(t *testing.T) {
	podAnnotations := make(map[string]string)
	podGroupName := fmt.Sprintf("podgroup-%v", "UID")
	podAnnotations[v1beta1.KubeGroupNameAnnotationKey] = podGroupName
	namespace := "default"

	podLabelsQuotaEnabled := make(map[string]string)
	podLabelsQuotaEnabled[quotaAllocatedLabelKey] = "true"
	podLabelsQuotaEnabled[quotaNameLabelKey] = "test"
	preemptorPod := buildPod(namespace, "preemptor-pod", "", "podUID0", v1.PodPending,
		buildResourceList("5000m", "50G"), nil, podLabelsQuotaEnabled, podAnnotations)

	podLabelsPreemptionMarked := make(map[string]string)
	podLabelsPreemptionMarked[quotaPreemptableLabelKey] = "true"
	podLabelsPreemptionMarked[quotaNameLabelKey] = "test"
	victimPod1 := buildPod(namespace, "victim-pod-1", "node1", "podUID1", v1.PodRunning,
		buildResourceList("5000m", "50G"), nil, podLabelsPreemptionMarked, podAnnotations)
	victimPod2 := buildPod(namespace, "victim-pod-2", "node2", "podUID2", v1.PodRunning,
		buildResourceList("5000m", "50G"), nil, podLabelsPreemptionMarked, podAnnotations)
	podNotMarkedForPreemption := buildPod(namespace, "pod-not-preempted", "", "podUID3", v1.PodPending,
		buildResourceList("5000m", "50G"), nil, podLabelsQuotaEnabled, podAnnotations)
	pendindPod := buildPod(namespace, "pending-pod", "", "podUID4", v1.PodPending,
		buildResourceList("5000m", "50G"), nil, podLabelsPreemptionMarked, podAnnotations)
	succeededPod := buildPod(namespace, "pending-pod", "node-3", "podUID5", v1.PodSucceeded,
		buildResourceList("5000m", "50G"), nil, podLabelsPreemptionMarked, podAnnotations)

	preempteeTasks := []*api.TaskInfo{api.NewTaskInfo(victimPod1), api.NewTaskInfo(victimPod2),
		api.NewTaskInfo(podNotMarkedForPreemption)}

	uqmPlugin := &uqmPlugin{}
	enabledPreemptable := true
	enabledPreemptableNodes := true
	enabledPreemptableTasks := true
	pluginOption := conf.PluginOption{
		Name:                    PluginName,
		EnabledPreemptable:      &enabledPreemptable,
		EnabledPreemptableNodes: &enabledPreemptableNodes,
		EnabledPreemptableTasks: &enabledPreemptableTasks,
	}
	schedulerCache := cache.NewDefaultMockSchedulerCache("test-scheduler")
	ssn := framework.OpenSession(schedulerCache, []conf.Tier{
		{
			Plugins: []conf.PluginOption{pluginOption},
		},
	},
		[]conf.Configuration{
			{
				Name: "preempt",
			},
		},
	)
	// Splitting 5 tasks accross 5 jobs. (2 tasks are preemptable)
	ssn.Jobs["job1"] = api.NewJobInfo("job1-UUID", api.NewTaskInfo(victimPod1))
	ssn.Jobs["job1"].AddTaskInfo(api.NewTaskInfo(victimPod1))
	ssn.Jobs["job2"] = api.NewJobInfo("job2-UUID", api.NewTaskInfo(victimPod2))
	ssn.Jobs["job2"].AddTaskInfo(api.NewTaskInfo(victimPod2))
	ssn.Jobs["job3"] = api.NewJobInfo("job3-UUID", api.NewTaskInfo(podNotMarkedForPreemption))
	ssn.Jobs["job3"].AddTaskInfo(api.NewTaskInfo(podNotMarkedForPreemption))
	ssn.Jobs["job4"] = api.NewJobInfo("job4-UUID", api.NewTaskInfo(pendindPod))
	ssn.Jobs["job4"].AddTaskInfo(api.NewTaskInfo(pendindPod))
	ssn.Jobs["job5"] = api.NewJobInfo("job4-UUID", api.NewTaskInfo(succeededPod))
	ssn.Jobs["job5"].AddTaskInfo(api.NewTaskInfo(succeededPod))

	assert.Assert(len(ssn.Jobs["job1"].TaskStatusIndex[api.Running]) == 1, "Job 1 Running pod")
	assert.Assert(len(ssn.Jobs["job2"].TaskStatusIndex[api.Running]) == 1, "Job 2 Running pod")
	assert.Assert(len(ssn.Jobs["job3"].TaskStatusIndex[api.Pending]) == 1, "Job 3 Pending pod")
	assert.Assert(len(ssn.Jobs["job4"].TaskStatusIndex[api.Pending]) == 1, "Job 4 Pending pod")
	assert.Assert(len(ssn.Jobs["job5"].TaskStatusIndex[api.Succeeded]) == 1, "Job 5 Succeeded pod")

	uqmPlugin.OnSessionOpen(ssn)
	victimTasks := ssn.Preemptable(api.NewTaskInfo(preemptorPod), preempteeTasks)
	assert.Assert(len(victimTasks) == 2, "expected 2 victims")
	for _, victim := range victimTasks {
		assert.Assert(victim.Pod.Name == "victim-pod-1" || victim.Pod.Name == "victim-pod-2", "expected victim pod")
	}
	assert.Assert(len(preemptableNodes) == 2, "expected preemptable nodes to be populated")
	nodes := []*api.NodeInfo{{Name: "node1"}, {Name: "node2"}, {Name: "node3"}, {Name: "node4"}, {Name: "node5"}}
	assert.Assert(len(ssn.GetPreemptableNodes(nodes)) == 2, "expected GetPreemptableNodes to return preemptable nodes")
	assert.Assert(len(ssn.GetPreemptableTasks()) == 2, "expected GetPreemptableTasks to return preemptable tasks")
}

func Test_TaskOrderFn(t *testing.T) {
	podAnnotations := make(map[string]string)
	podGroupName := fmt.Sprintf("podgroup-%v", "UID")
	podAnnotations[v1beta1.KubeGroupNameAnnotationKey] = podGroupName
	namespace := "default"

	// Create pod with UNDER_GUARANTEED and early timestamp
	podLabelsUnderEarly := make(map[string]string)
	podLabelsUnderEarly[quotaAllocatedLabelKey] = "true"
	podLabelsUnderEarly[quotaNameLabelKey] = "test"
	podLabelsUnderEarly[quotaAllocatedFromLabel] = underGuaranteedState
	podLabelsUnderEarly[quotaAllocatedAtLabelKey] = "1733230800"
	podUnderEarly := buildPod(namespace, "under-early-pod", "", "podUID1", v1.PodPending,
		buildResourceList("5000m", "50G"), nil, podLabelsUnderEarly, podAnnotations)

	// Create pod with UNDER_GUARANTEED and late timestamp
	podLabelsUnderLate := make(map[string]string)
	podLabelsUnderLate[quotaAllocatedLabelKey] = "true"
	podLabelsUnderLate[quotaNameLabelKey] = "test"
	podLabelsUnderLate[quotaAllocatedFromLabel] = underGuaranteedState
	podLabelsUnderLate[quotaAllocatedAtLabelKey] = "1733230900"
	podUnderLate := buildPod(namespace, "under-late-pod", "", "podUID2", v1.PodPending,
		buildResourceList("5000m", "50G"), nil, podLabelsUnderLate, podAnnotations)

	// Create pod with OVER_GUARANTEED and early timestamp
	podLabelsOverEarly := make(map[string]string)
	podLabelsOverEarly[quotaAllocatedLabelKey] = "true"
	podLabelsOverEarly[quotaNameLabelKey] = "test"
	podLabelsOverEarly[quotaAllocatedFromLabel] = overGuaranteedState
	podLabelsOverEarly[quotaAllocatedAtLabelKey] = "1733230700"
	podOverEarly := buildPod(namespace, "over-early-pod", "", "podUID3", v1.PodPending,
		buildResourceList("5000m", "50G"), nil, podLabelsOverEarly, podAnnotations)

	// Create pod with UNDER_GUARANTEED but no timestamp
	podLabelsUnderNoTime := make(map[string]string)
	podLabelsUnderNoTime[quotaAllocatedLabelKey] = "true"
	podLabelsUnderNoTime[quotaNameLabelKey] = "test"
	podLabelsUnderNoTime[quotaAllocatedFromLabel] = underGuaranteedState
	podUnderNoTime := buildPod(namespace, "under-notime-pod", "", "podUID4", v1.PodPending,
		buildResourceList("5000m", "50G"), nil, podLabelsUnderNoTime, podAnnotations)

	uqmPlugin := &uqmPlugin{}
	enabledTaskOrder := true
	pluginOption := conf.PluginOption{
		Name:             PluginName,
		EnabledTaskOrder: &enabledTaskOrder,
	}
	schedulerCache := cache.NewDefaultMockSchedulerCache("test-scheduler")
	ssn := framework.OpenSession(schedulerCache, []conf.Tier{
		{
			Plugins: []conf.PluginOption{pluginOption},
		},
	},
		[]conf.Configuration{
			{
				Name: "allocate",
			},
		},
	)
	uqmPlugin.OnSessionOpen(ssn)

	taskUnderEarly := api.NewTaskInfo(podUnderEarly)
	taskUnderLate := api.NewTaskInfo(podUnderLate)
	taskOverEarly := api.NewTaskInfo(podOverEarly)
	taskUnderNoTime := api.NewTaskInfo(podUnderNoTime)

	type args struct {
		left  *api.TaskInfo
		right *api.TaskInfo
	}

	tests := []struct {
		name           string
		args           args
		expectedResult bool // true = left has higher priority, false = right has higher priority or equal
		failureMessage string
	}{
		{
			name: "UNDER_early_vs_UNDER_late",
			args: args{
				left:  taskUnderEarly,
				right: taskUnderLate,
			},
			expectedResult: true,
			failureMessage: "expected UNDER with earlier timestamp to have higher priority",
		},
		{
			name: "UNDER_late_vs_UNDER_early",
			args: args{
				left:  taskUnderLate,
				right: taskUnderEarly,
			},
			expectedResult: false,
			failureMessage: "expected UNDER with earlier timestamp to have higher priority",
		},
		{
			name: "UNDER_vs_OVER_earlier_timestamp",
			args: args{
				left:  taskUnderEarly,
				right: taskOverEarly,
			},
			expectedResult: true,
			failureMessage: "expected UNDER to have higher priority than OVER regardless of timestamp",
		},
		{
			name: "OVER_vs_UNDER",
			args: args{
				left:  taskOverEarly,
				right: taskUnderEarly,
			},
			expectedResult: false,
			failureMessage: "expected UNDER to have higher priority than OVER",
		},
		{
			name: "UNDER_with_timestamp_vs_UNDER_without_timestamp",
			args: args{
				left:  taskUnderEarly,
				right: taskUnderNoTime,
			},
			expectedResult: true,
			failureMessage: "expected UNDER with timestamp to have higher priority than without",
		},
		{
			name: "UNDER_without_timestamp_vs_UNDER_with_timestamp",
			args: args{
				left:  taskUnderNoTime,
				right: taskUnderEarly,
			},
			expectedResult: false,
			failureMessage: "expected UNDER with timestamp to have higher priority than without",
		},
		{
			name: "OVER_vs_OVER_no_timestamp_comparison",
			args: args{
				left:  taskOverEarly,
				right: taskOverEarly,
			},
			expectedResult: false,
			failureMessage: "expected same priority for OVER tasks (no timestamp comparison)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ssn.TaskOrderFn(tt.args.left, tt.args.right)
			assert.Assert(result == tt.expectedResult, tt.failureMessage)
		})
	}
}

func TestJobOrderFn(t *testing.T) {
	// Create test session
	uqmPlugin := &uqmPlugin{}
	enabledJobOrder := true
	pluginOption := conf.PluginOption{
		Name:            PluginName,
		EnabledJobOrder: &enabledJobOrder,
	}
	schedulerCache := cache.NewDefaultMockSchedulerCache("test-scheduler")
	ssn := framework.OpenSession(schedulerCache, []conf.Tier{
		{
			Plugins: []conf.PluginOption{pluginOption},
		},
	},
		[]conf.Configuration{
			{
				Name: "allocate",
			},
		},
	)
	uqmPlugin.OnSessionOpen(ssn)

	// Build test jobs
	jobWithUnderEarly := buildJob("default", "job1", buildPodWithLabels("default", "pod1", map[string]string{
		quotaAllocatedFromLabel:  underGuaranteedState,
		quotaAllocatedAtLabelKey: "1733230800", // early
	}))

	jobWithUnderLate := buildJob("default", "job2", buildPodWithLabels("default", "pod2", map[string]string{
		quotaAllocatedFromLabel:  underGuaranteedState,
		quotaAllocatedAtLabelKey: "1733230900", // late
	}))

	jobWithOver := buildJob("default", "job3", buildPodWithLabels("default", "pod3", map[string]string{
		quotaAllocatedFromLabel:  overGuaranteedState,
		quotaAllocatedAtLabelKey: "1733230700", // earlier but OVER
	}))

	jobWithoutTimestamp := buildJob("default", "job4", buildPodWithLabels("default", "pod4", map[string]string{
		quotaAllocatedFromLabel: underGuaranteedState,
		// no timestamp
	}))

	jobWithMultiplePods := buildJob("default", "job5",
		buildPodWithLabels("default", "pod5a", map[string]string{
			quotaAllocatedFromLabel:  underGuaranteedState,
			quotaAllocatedAtLabelKey: "1733230850",
		}),
		buildPodWithLabels("default", "pod5b", map[string]string{
			quotaAllocatedFromLabel:  underGuaranteedState,
			quotaAllocatedAtLabelKey: "1733230820", // earliest in this job
		}),
		buildPodWithLabels("default", "pod5c", map[string]string{
			quotaAllocatedFromLabel:  overGuaranteedState,
			quotaAllocatedAtLabelKey: "1733230700", // ignored (OVER)
		}),
	)

	type args struct {
		left  *api.JobInfo
		right *api.JobInfo
	}

	tests := []struct {
		name           string
		args           args
		expectedResult bool // true = left has higher priority, false = right has higher priority or equal
		failureMessage string
	}{
		{
			name: "UNDER_early_vs_UNDER_late",
			args: args{
				left:  jobWithUnderEarly,
				right: jobWithUnderLate,
			},
			expectedResult: true,
			failureMessage: "expected UNDER with earlier timestamp to have higher priority",
		},
		{
			name: "UNDER_late_vs_UNDER_early",
			args: args{
				left:  jobWithUnderLate,
				right: jobWithUnderEarly,
			},
			expectedResult: false,
			failureMessage: "expected UNDER with earlier timestamp to have higher priority",
		},
		{
			name: "UNDER_vs_OVER_earlier_timestamp",
			args: args{
				left:  jobWithUnderEarly,
				right: jobWithOver,
			},
			expectedResult: true,
			failureMessage: "expected UNDER to have higher priority than OVER regardless of timestamp",
		},
		{
			name: "OVER_vs_UNDER",
			args: args{
				left:  jobWithOver,
				right: jobWithUnderEarly,
			},
			expectedResult: false,
			failureMessage: "expected UNDER to have higher priority than OVER",
		},
		{
			name: "UNDER_with_timestamp_vs_UNDER_without_timestamp",
			args: args{
				left:  jobWithUnderEarly,
				right: jobWithoutTimestamp,
			},
			expectedResult: true,
			failureMessage: "expected UNDER with timestamp to have higher priority",
		},
		{
			name: "job_with_multiple_pods_uses_earliest_UNDER_timestamp",
			args: args{
				left:  jobWithMultiplePods, // earliest UNDER: 1733230820
				right: jobWithUnderEarly,   // 1733230800
			},
			expectedResult: false,
			failureMessage: "expected job to use earliest UNDER timestamp (1733230800 < 1733230820)",
		},
		{
			name: "same_timestamps",
			args: args{
				left:  jobWithUnderEarly,
				right: jobWithUnderEarly,
			},
			expectedResult: false,
			failureMessage: "expected same priority for same timestamps",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ssn.JobOrderFn(tt.args.left, tt.args.right)
			assert.Assert(result == tt.expectedResult, tt.failureMessage)
		})
	}
}

// Helper function to build a pod with specific labels
func buildPodWithLabels(namespace, name string, labels map[string]string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(fmt.Sprintf("%s-%s", namespace, name)),
			Labels:    labels,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: buildResourceList("1000m", "1G"),
					},
				},
			},
		},
	}
}

// Helper function to build a job with multiple pods
func buildJob(namespace, name string, pods ...*v1.Pod) *api.JobInfo {
	if len(pods) == 0 {
		return nil
	}

	// Convert pods to tasks
	tasks := make([]*api.TaskInfo, len(pods))
	for i, pod := range pods {
		tasks[i] = api.NewTaskInfo(pod)
	}

	// Create job with all tasks at once
	jobUID := api.JobID(types.UID(fmt.Sprintf("%s-%s", namespace, name)))
	job := api.NewJobInfo(jobUID, tasks...)
	job.Name = name
	job.Namespace = namespace

	return job
}

func TestCompareTimestamps(t *testing.T) {
	ts1 := int64(1733230800)
	ts2 := int64(1733230900)
	ts3 := int64(1733230800)

	// Test case 1: Both have timestamps - left earlier
	result := compareTimestamps(&ts1, &ts2)
	assert.Assert(result == -1, "expected left to have higher priority when earlier")

	// Test case 2: Both have timestamps - right earlier
	result = compareTimestamps(&ts2, &ts1)
	assert.Assert(result == 1, "expected right to have higher priority when earlier")

	// Test case 3: Both have timestamps - equal
	result = compareTimestamps(&ts1, &ts3)
	assert.Assert(result == 0, "expected same priority for equal timestamps")

	// Test case 4: Only left has timestamp
	result = compareTimestamps(&ts1, nil)
	assert.Assert(result == -1, "expected left to have higher priority when only left has timestamp")

	// Test case 5: Only right has timestamp
	result = compareTimestamps(nil, &ts1)
	assert.Assert(result == 1, "expected right to have higher priority when only right has timestamp")

	// Test case 6: Neither has timestamp
	result = compareTimestamps(nil, nil)
	assert.Assert(result == 0, "expected same priority when neither has timestamp")

	// Test case 7: Large timestamp difference
	tsSmall := int64(999999999)
	tsLarge := int64(1733230800)
	result = compareTimestamps(&tsSmall, &tsLarge)
	assert.Assert(result == -1, "expected numerical comparison (999999999 < 1733230800)")
}

func TestParseTimestampLabel(t *testing.T) {
	// Test case 1: Valid timestamp
	timestamp, ok := parseTimestampLabel("1733230800")
	assert.Assert(ok, "expected parsing to succeed for valid timestamp")
	assert.Assert(timestamp == 1733230800, "expected parsed timestamp to match input")

	// Test case 2: Another valid timestamp
	timestamp, ok = parseTimestampLabel("1234567890")
	assert.Assert(ok, "expected parsing to succeed for valid timestamp")
	assert.Assert(timestamp == 1234567890, "expected parsed timestamp to match input")

	// Test case 3: Zero timestamp (valid edge case)
	timestamp, ok = parseTimestampLabel("0")
	assert.Assert(ok, "expected parsing to succeed for zero timestamp")
	assert.Assert(timestamp == 0, "expected parsed timestamp to be 0")

	// Test case 4: Invalid format - special characters
	timestamp, ok = parseTimestampLabel("123.456")
	assert.Assert(!ok, "expected parsing to fail for decimal number")
	assert.Assert(timestamp == 0, "expected timestamp to be 0 on failure")
}

func TestFormatTimestamp(t *testing.T) {
	// Test case 1: Valid timestamp
	ts1 := int64(1733230800)
	result := formatTimestamp(&ts1)
	assert.Assert(result == "1733230800", "expected formatted timestamp to match value")

	// Test case 2: Zero timestamp
	ts2 := int64(0)
	result = formatTimestamp(&ts2)
	assert.Assert(result == "0", "expected formatted zero timestamp")

	// Test case 3: Negative timestamp
	ts3 := int64(-100)
	result = formatTimestamp(&ts3)
	assert.Assert(result == "-100", "expected formatted negative timestamp")

	// Test case 4: Large timestamp
	ts4 := int64(9999999999)
	result = formatTimestamp(&ts4)
	assert.Assert(result == "9999999999", "expected formatted large timestamp")

	// Test case 5: Nil pointer
	result = formatTimestamp(nil)
	assert.Assert(result == "<nil>", "expected <nil> for nil pointer")
}

func TestGetJobEarliestTimestamp(t *testing.T) {
	// Test case 1: Job with no sorted task list
	job := api.NewJobInfo("job1")
	jobEarliestTimestamps = make(map[string]*sortedTaskList)
	result := getJobEarliestTimestamp(job)
	assert.Assert(result == nil, "expected nil for job with no sorted task list")

	// Test case 2: Job with empty sorted task list
	jobEarliestTimestamps[string(job.UID)] = &sortedTaskList{
		tasks:     []TaskTimestamp{},
		nextIndex: 0,
	}
	result = getJobEarliestTimestamp(job)
	assert.Assert(result == nil, "expected nil when sorted task list is empty")

	// Test case 3: Job with single task in sorted list - task is still pending
	pod1 := buildPod("default", "pod1", "", "pod1-uid", v1.PodPending,
		buildResourceList("1000m", "1G"), nil, make(map[string]string), make(map[string]string))
	task1 := api.NewTaskInfo(pod1)
	job = api.NewJobInfo("job3", task1)

	jobEarliestTimestamps[string(job.UID)] = &sortedTaskList{
		tasks: []TaskTimestamp{
			{TaskUID: task1.UID, Timestamp: 1733230800},
		},
		nextIndex: 0,
	}
	result = getJobEarliestTimestamp(job)
	assert.Assert(result != nil && *result == 1733230800, "expected timestamp 1733230800 for single pending task")
	assert.Assert(jobEarliestTimestamps[string(job.UID)].nextIndex == 0, "expected nextIndex to be updated to 0")

	// Test case 4: Job with multiple tasks - returns earliest pending
	pod2 := buildPod("default", "pod2", "", "pod2-uid", v1.PodPending,
		buildResourceList("1000m", "1G"), nil, make(map[string]string), make(map[string]string))
	pod3 := buildPod("default", "pod3", "", "pod3-uid", v1.PodPending,
		buildResourceList("1000m", "1G"), nil, make(map[string]string), make(map[string]string))
	task2 := api.NewTaskInfo(pod2)
	task3 := api.NewTaskInfo(pod3)
	job = api.NewJobInfo("job4", task1, task2, task3)

	jobEarliestTimestamps[string(job.UID)] = &sortedTaskList{
		tasks: []TaskTimestamp{
			{TaskUID: task3.UID, Timestamp: 1733230700}, // Earliest
			{TaskUID: task1.UID, Timestamp: 1733230800},
			{TaskUID: task2.UID, Timestamp: 1733230900},
		},
		nextIndex: 0,
	}
	result = getJobEarliestTimestamp(job)
	assert.Assert(result != nil && *result == 1733230700, "expected earliest timestamp 1733230700")

	// Test case 5: Lazy deletion - first task is no longer pending
	job = api.NewJobInfo("job5", task2, task3)
	job.UpdateTaskStatus(task3, api.Bound) // task3 is no longer pending

	jobEarliestTimestamps[string(job.UID)] = &sortedTaskList{
		tasks: []TaskTimestamp{
			{TaskUID: task3.UID, Timestamp: 1733230700}, // Not pending (should be skipped)
			{TaskUID: task2.UID, Timestamp: 1733230900}, // Pending
		},
		nextIndex: 0,
	}
	result = getJobEarliestTimestamp(job)
	assert.Assert(result != nil && *result == 1733230900, "expected to skip non-pending task and return 1733230900")
	assert.Assert(jobEarliestTimestamps[string(job.UID)].nextIndex == 1, "expected nextIndex to be updated to 1")

	// Test case 6: All tasks in list are no longer pending
	job = api.NewJobInfo("job6")
	jobEarliestTimestamps[string(job.UID)] = &sortedTaskList{
		tasks: []TaskTimestamp{
			{TaskUID: task1.UID, Timestamp: 1733230800},
			{TaskUID: task2.UID, Timestamp: 1733230900},
		},
		nextIndex: 0,
	}
	result = getJobEarliestTimestamp(job)
	assert.Assert(result == nil, "expected nil when all tasks are no longer pending")

	// Test case 7: nextIndex optimization - start from middle
	pod4 := buildPod("default", "pod4", "", "pod4-uid", v1.PodPending,
		buildResourceList("1000m", "1G"), nil, make(map[string]string), make(map[string]string))
	task4 := api.NewTaskInfo(pod4)
	job = api.NewJobInfo("job7", task2, task4)

	jobEarliestTimestamps[string(job.UID)] = &sortedTaskList{
		tasks: []TaskTimestamp{
			{TaskUID: task1.UID, Timestamp: 1733230700}, // Already processed
			{TaskUID: task3.UID, Timestamp: 1733230800}, // Already processed
			{TaskUID: task2.UID, Timestamp: 1733230900}, // Pending
			{TaskUID: task4.UID, Timestamp: 1733231000}, // Pending
		},
		nextIndex: 2, // Start from index 2
	}
	result = getJobEarliestTimestamp(job)
	assert.Assert(result != nil && *result == 1733230900, "expected to start from nextIndex and return 1733230900")
	assert.Assert(jobEarliestTimestamps[string(job.UID)].nextIndex == 2, "expected nextIndex to remain at 2")
}

func TestPreProcessUnderGuaranteedTimestamps(t *testing.T) {
	// Create test pods with various states
	podLabelsUnder1 := make(map[string]string)
	podLabelsUnder1[quotaAllocatedFromLabel] = underGuaranteedState
	podLabelsUnder1[quotaAllocatedAtLabelKey] = "1733230800"
	pod1 := buildPod("default", "pod1", "", "pod1-uid", v1.PodPending,
		buildResourceList("1000m", "1G"), nil, podLabelsUnder1, make(map[string]string))

	podLabelsUnder2 := make(map[string]string)
	podLabelsUnder2[quotaAllocatedFromLabel] = underGuaranteedState
	podLabelsUnder2[quotaAllocatedAtLabelKey] = "1733230900"
	pod2 := buildPod("default", "pod2", "", "pod2-uid", v1.PodRunning,
		buildResourceList("1000m", "1G"), nil, podLabelsUnder2, make(map[string]string))

	podLabelsUnder3 := make(map[string]string)
	podLabelsUnder3[quotaAllocatedFromLabel] = underGuaranteedState
	podLabelsUnder3[quotaAllocatedAtLabelKey] = "1733230700"
	pod3Under := buildPod("default", "pod3", "", "pod3-uid", v1.PodPending,
		buildResourceList("1000m", "1G"), nil, podLabelsUnder3, make(map[string]string))

	podLabelsOver := make(map[string]string)
	podLabelsOver[quotaAllocatedFromLabel] = overGuaranteedState
	podLabelsOver[quotaAllocatedAtLabelKey] = "1733230600"
	pod4 := buildPod("default", "pod4", "", "pod4-uid", v1.PodPending,
		buildResourceList("1000m", "1G"), nil, podLabelsOver, make(map[string]string))

	podLabelsInvalid := make(map[string]string)
	podLabelsInvalid[quotaAllocatedFromLabel] = underGuaranteedState
	podLabelsInvalid[quotaAllocatedAtLabelKey] = "invalid"
	pod5 := buildPod("default", "pod5", "", "pod5-uid", v1.PodPending,
		buildResourceList("1000m", "1G"), nil, podLabelsInvalid, make(map[string]string))

	// Create tasks
	task1 := api.NewTaskInfo(pod1)
	task2 := api.NewTaskInfo(pod2)
	task3Under := api.NewTaskInfo(pod3Under)
	task4 := api.NewTaskInfo(pod4)
	task5 := api.NewTaskInfo(pod5)

	// Create job with all tasks
	job := api.NewJobInfo("job1", task1, task2, task3Under, task4, task5)

	// Update task2 status to Running
	job.UpdateTaskStatus(task2, api.Running)

	ssn := &framework.Session{
		Jobs: map[api.JobID]*api.JobInfo{
			job.UID: job,
		},
	}

	// Run preprocessing
	preProcessUnderGuaranteedTimestamps(ssn)

	// Verify task-level map
	assert.Assert(len(taskUnderGuaranteedTimestamps) == 3, "expected 3 tasks with valid UNDER_GUARANTEED timestamps")
	assert.Assert(taskUnderGuaranteedTimestamps["pod1-uid"] == 1733230800, "expected pod1 timestamp")
	assert.Assert(taskUnderGuaranteedTimestamps["pod2-uid"] == 1733230900, "expected pod2 timestamp")
	assert.Assert(taskUnderGuaranteedTimestamps["pod3-uid"] == 1733230700, "expected pod3 timestamp")
	_, exists := taskUnderGuaranteedTimestamps["pod4-uid"]
	assert.Assert(!exists, "expected pod4 (OVER_GUARANTEED) to not be in map")
	_, exists = taskUnderGuaranteedTimestamps["pod5-uid"]
	assert.Assert(!exists, "expected pod5 (invalid timestamp) to not be in map")

	// Verify job-level sorted list
	assert.Assert(len(jobEarliestTimestamps) == 1, "expected 1 job with under-guaranteed pending tasks")
	jobList, exists := jobEarliestTimestamps[string(job.UID)]
	assert.Assert(exists, "expected job to have sorted task list")
	assert.Assert(len(jobList.tasks) == 2, "expected 2 pending under-guaranteed tasks (pod1 and pod3)")
	assert.Assert(jobList.nextIndex == 0, "expected nextIndex to start at 0")

	// Verify tasks are sorted by timestamp (earliest first)
	assert.Assert(jobList.tasks[0].TaskUID == task3Under.UID, "expected pod3 (ts=700) to be first")
	assert.Assert(jobList.tasks[0].Timestamp == 1733230700, "expected earliest timestamp 700")
	assert.Assert(jobList.tasks[1].TaskUID == task1.UID, "expected pod1 (ts=800) to be second")
	assert.Assert(jobList.tasks[1].Timestamp == 1733230800, "expected second timestamp 800")
}
