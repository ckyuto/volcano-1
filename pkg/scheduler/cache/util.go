/*
Copyright 2021 The Volcano Authors.

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

package cache

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"stathat.com/c/consistent"

	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

type hyperNodeEventSource string

const (
	hyperNodeEventSourceNode      hyperNodeEventSource = "node"
	hyperNodeEventSourceHyperNode hyperNodeEventSource = "hyperNode"

	schedulerGroupPrefix = "scheduler-group-"
)

// ConsistentInterface defines the methods for interacting with a consistent hashing
// system that can retrieve values by key and add new schedulers to the system.
//
// The interface was designed to help with testing by allowing you to mock the
// behavior of a consistent hashing system. In production, an implementation like
// `consistent.Consistent` will be used, but for unit tests, you can use a mock
// implementation to simulate the behavior of the methods without relying on a real system.
type ConsistentInterface interface {
	// Get retrieves the value associated with the given key.
	// Returns the value as a string and an error if the key cannot be found.
	Get(key string) (string, error)

	// Add adds a new name to the consistent hashing system.
	// This method is used to add new schedulers to the cluster.
	Add(name string)
}

// GetSchedulerGroup determines the scheduler group for a given pod based on its index and environment variables.
func GetSchedulerGroup(podName string) (string, error) {
	// Validate environment variables for scheduler group configuration
	replicaNum, schedulerGroupNum, err := validateEnvVariables()
	if err != nil {
		return "", err
	}

	// Extract the pod index from the pod name
	index, err := getIndexFromPodName(podName)
	if err != nil {
		return "", fmt.Errorf("failed to extract index from pod name %s: %v", podName, err)
	}

	// Determine the group based on the pod index
	groupIndex := index / replicaNum

	// Ensure the group index is within valid bounds
	if groupIndex >= schedulerGroupNum {
		return "", fmt.Errorf("group index %d exceeds the number of scheduler groups: %d", groupIndex, schedulerGroupNum)
	}
	return fmt.Sprintf("%s%d", schedulerGroupPrefix, groupIndex), nil
}

// validateEnvVariables validates the necessary environment variables for scheduler group configuration.
// It checks if the `REPLICA_PER_SCHEDULER_GROUP` and `SCHEDULER_GROUP_NUM` environment variables are
// set correctly, returning the parsed values or an error if invalid.
func validateEnvVariables() (replicaNum int, schedulerGroupNum int, err error) {
	// Validate and get the number of replicas per scheduler group
	replicaNumStr := os.Getenv("REPLICA_PER_SCHEDULER_GROUP")
	replicaNum, err = strconv.Atoi(replicaNumStr)
	if err != nil || replicaNum <= 0 {
		err = fmt.Errorf("invalid replica number in environment variable REPLICA_PER_SCHEDULER_GROUP: %s", replicaNumStr)
		return 0, 0, err
	}

	// Validate and get the total number of scheduler groups
	schedulerGroupNumStr := os.Getenv("SCHEDULER_GROUP_NUM")
	schedulerGroupNum, err = strconv.Atoi(schedulerGroupNumStr)
	if err != nil || schedulerGroupNum <= 0 {
		err = fmt.Errorf("invalid scheduler group number in environment variable SCHEDULER_GROUP_NUM: %s", schedulerGroupNumStr)
		return 0, 0, err
	}

	return replicaNum, schedulerGroupNum, nil
}

// getIndexFromPodName extracts the index from the pod name, assuming it's the last part of the name after the last dash.
func getIndexFromPodName(podName string) (int, error) {
	parts := strings.Split(podName, "-")
	if len(parts) == 0 {
		return 0, fmt.Errorf("pod name %s is invalid", podName)
	}
	return strconv.Atoi(parts[len(parts)-1])
}

// responsibleForPod returns false at following conditions:
// 1. The current scheduler is not specified scheduler in Pod's spec.
// 2. The Job which the Pod belongs is not assigned to current scheduler based on the hash algorithm in multi-schedulers scenario
func responsibleForPod(pod *v1.Pod, schedulerNames []string, mySchedulerPodName string, c *consistent.Consistent) bool {
	if !slices.Contains(schedulerNames, pod.Spec.SchedulerName) {
		return false
	}
	if c != nil {
		var key string
		if len(pod.OwnerReferences) != 0 {
			key = pod.OwnerReferences[0].Name
		} else {
			key = pod.Name
		}
		// Get the scheduler group name for the pod based on the hash algorithm
		schedulerGroupName, err := c.Get(key)
		if err != nil {
			klog.Errorf("Failed to get scheduler by hash algorithm, err: %v", err)
		}
		// Get the scheduler group name for the current scheduler pod
		mySchedulerGroupName, err := GetSchedulerGroup(mySchedulerPodName)
		if err != nil {
			klog.Errorf("Error determining scheduler group: %v", err)
			return false
		}
		if schedulerGroupName != mySchedulerGroupName {
			return false
		}
	}

	klog.V(4).Infof("schedulerPodName %v is responsible to Pod %v/%v", mySchedulerPodName, pod.Namespace, pod.Name)
	return true
}

// responsibleForNode returns true if the Node is assigned to current scheduler in multi-scheduler scenario
func responsibleForNode(nodeName string, mySchedulerPodName string, c *consistent.Consistent) bool {
	if c != nil {
		// Get the scheduler group name for the node based on the hash algorithm
		schedulerGroupName, err := c.Get(nodeName)
		if err != nil {
			klog.Errorf("Failed to get scheduler by hash algorithm, err: %v", err)
		}

		// Get the scheduler group name for the current scheduler pod
		mySchedulerGroupName, err := GetSchedulerGroup(mySchedulerPodName)
		if err != nil {
			klog.Errorf("Error determining scheduler group: %v\n", err)
			return false
		}
		if schedulerGroupName != mySchedulerGroupName {
			return false
		}
	}

	klog.V(4).Infof("schedulerPodName %v is responsible to Node %v", mySchedulerPodName, nodeName)
	return true
}

// responsibleForPodGroup returns true if Job which PodGroup belongs is assigned to current scheduler in multi-schedulers scenario
func responsibleForPodGroup(pg *scheduling.PodGroup, mySchedulerPodName string, c *consistent.Consistent) bool {
	if c != nil {
		var key string
		if len(pg.OwnerReferences) != 0 {
			key = pg.OwnerReferences[0].Name
		} else {
			key = pg.Name
		}
		// Get the scheduler group name for the podGroup based on the hash algorithm
		schedulerGroupName, err := c.Get(key)
		if err != nil {
			klog.Errorf("Failed to get scheduler by hash algorithm, err: %v", err)
		}
		// Get the scheduler group name for the current scheduler pod
		mySchedulerGroupName, err := GetSchedulerGroup(mySchedulerPodName)
		if err != nil {
			klog.Errorf("Error determining scheduler group: %v\n", err)
			return false
		}
		if schedulerGroupName != mySchedulerGroupName {
			return false
		}
	}

	klog.V(4).Infof("schedulerPodName %v is responsible to PodGroup %v/%v", mySchedulerPodName, pg.Namespace, pg.Name)
	return true
}

// getMultiSchedulerInfo return the Pod name of current scheduler and the hash table for all schedulers
func getMultiSchedulerInfo() (schedulerPodName string, c *consistent.Consistent) {
	multiSchedulerEnable := os.Getenv("MULTI_SCHEDULER_ENABLE")
	mySchedulerPodName := os.Getenv("SCHEDULER_POD_NAME")
	c = nil
	if multiSchedulerEnable == "true" {
		klog.V(3).Infof("multiSchedulerEnable true")
		schedulerGroupNumStr := os.Getenv("SCHEDULER_GROUP_NUM")
		schedulerGroupNum, err := strconv.Atoi(schedulerGroupNumStr)
		if err != nil {
			schedulerGroupNum = 1
		}
		c = consistent.New()
		for i := 0; i < schedulerGroupNum; i++ {
			name := fmt.Sprintf("%s%d", schedulerGroupPrefix, i)
			c.Add(name)
		}
	}
	return mySchedulerPodName, c
}

func getHyperNodeEventSource(source string) []string {
	parts := strings.Split(source, "/")
	if len(parts) != 2 {
		return nil
	}
	return parts
}
