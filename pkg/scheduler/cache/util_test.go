package cache

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

// Test responsibleForPod function
func TestResponsibleForPod(t *testing.T) {
	tests := []struct {
		name               string
		podName            string
		schedulerNames     []string
		mySchedulerPodName string
		expectedResult     bool
	}{
		{
			name:               "Pod belongs to same scheduler group",
			podName:            "pod-1",
			schedulerNames:     []string{"volcano"},
			mySchedulerPodName: func() string {
				schedulerGroupName, _ := globalConsistent.Get("pod-1")
				index, _ := getIndexFromPodName(schedulerGroupName)
				return fmt.Sprintf("pod-prefix-%d", index * REPLICA_PER_SCHEDULER_GROUP)
			}(),
			expectedResult:     true,
		},
		{
			name:               "Pod belongs to a different scheduler group",
			podName:            "pod-2",
			schedulerNames:     []string{"volcano"},
			mySchedulerPodName: func() string {
				schedulerGroupName, _ := globalConsistent.Get("pod-1")
				index, _ := getIndexFromPodName(schedulerGroupName)
				return fmt.Sprintf("pod-prefix-%d", index * REPLICA_PER_SCHEDULER_GROUP)
			}(),
			expectedResult:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: tt.podName,
				},
				Spec: v1.PodSpec{
					SchedulerName: "volcano",
				},
			}
			result := responsibleForPod(pod, tt.schedulerNames, tt.mySchedulerPodName, globalConsistent)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

// Test responsibleForNode function
func TestResponsibleForNode(t *testing.T) {
	tests := []struct {
		name               string
		nodeName           string
		mySchedulerPodName string
		expectedResult     bool
	}{
		{
			name:               "Node belongs to the same scheduler group",
			nodeName:           "node-1",
			mySchedulerPodName: func() string {
				schedulerGroupName, _ := globalConsistent.Get("node-1")
				index, _ := getIndexFromPodName(schedulerGroupName)
				return fmt.Sprintf("pod-prefix-%d", index * REPLICA_PER_SCHEDULER_GROUP)
			}(),

			expectedResult:     true,
		},
		{
			name:               "Node belongs to a different scheduler group",
			nodeName:           "node-2",
			mySchedulerPodName: func() string {
				schedulerGroupName, _ := globalConsistent.Get("node-1")
				index, _ := getIndexFromPodName(schedulerGroupName)
				return fmt.Sprintf("pod-prefix-%d", index * REPLICA_PER_SCHEDULER_GROUP)
			}(),
			expectedResult:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := responsibleForNode(tt.nodeName, tt.mySchedulerPodName, globalConsistent)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

// Test responsibleForPodGroup function
func TestResponsibleForPodGroup(t *testing.T) {
	tests := []struct {
		name               string
		pgName             string
		mySchedulerPodName string
		expectedResult     bool
	}{
		{
			name:               "PodGroup belongs to the same scheduler group",
			pgName:             "podgroup-1",
			mySchedulerPodName: func() string {
				schedulerGroupName, _ := globalConsistent.Get("podgroup-1")
				index, _ := getIndexFromPodName(schedulerGroupName)
				return fmt.Sprintf("pod-prefix-%d", index * REPLICA_PER_SCHEDULER_GROUP)
			}(),
			expectedResult:     true,
		},
		{
			name:               "PodGroup belongs to a different scheduler group",
			pgName:             "podgroup-2",
			mySchedulerPodName: func() string {
				schedulerGroupName, _ := globalConsistent.Get("podgroup-1")
				index, _ := getIndexFromPodName(schedulerGroupName)
				return fmt.Sprintf("pod-prefix-%d", index * REPLICA_PER_SCHEDULER_GROUP)
			}(),
			expectedResult:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pg := &scheduling.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name: tt.pgName,
				},
			}

			result := responsibleForPodGroup(pg, tt.mySchedulerPodName, globalConsistent)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

// Test getMultiSchedulerInfo function
func TestGetMultiSchedulerInfo(t *testing.T) {
	tests := []struct {
		name                        string
		multiSchedulerEnable        string
		envVars                     map[string]string
		expectedSchedulerPodName    string
		expectedSchedulerGroupCount int
	}{
		{
			name:                 "Multi scheduler enabled",
			multiSchedulerEnable: "true",
			envVars: map[string]string{
				"MULTI_SCHEDULER_ENABLE": "true",
				"SCHEDULER_POD_NAME":     "scheduler-0",
				"SCHEDULER_GROUP_NUM":    "3",
			},
			expectedSchedulerPodName:    "scheduler-0",
			expectedSchedulerGroupCount: 3,
		},
		{
			name:                 "Multi scheduler disabled",
			multiSchedulerEnable: "false",
			envVars: map[string]string{
				"MULTI_SCHEDULER_ENABLE": "false",
				"SCHEDULER_POD_NAME":     "scheduler-0",
			},
			expectedSchedulerPodName:    "scheduler-0",
			expectedSchedulerGroupCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set environment variables
			for k, v := range tt.envVars {
				os.Setenv(k, v)
			}

			schedulerPodName, c := getMultiSchedulerInfo()

			assert.Equal(t, tt.expectedSchedulerPodName, schedulerPodName)

			if tt.multiSchedulerEnable == "true" {
				assert.NotNil(t, c)
			} else {
				assert.Nil(t, c)
			}
		})
	}
}

// TestGetSchedulerGroup tests the GetSchedulerGroup function.
func TestGetSchedulerGroup(t *testing.T) {
	tests := []struct {
		name                     string
		podName                  string
		replicaPerSchedulerGroup string
		schedulerGroupNum        string
		expectedGroup            string
		expectedError            bool
	}{
		{
			name:                     "Valid case",
			podName:                  "pod-5",
			replicaPerSchedulerGroup: "2",
			schedulerGroupNum:        "3",
			expectedGroup:            "scheduler-group-2", // (5 / 2) = group 2
			expectedError:            false,
		},
		{
			name:                     "Invalid replica number",
			podName:                  "pod-5",
			replicaPerSchedulerGroup: "invalid",
			schedulerGroupNum:        "3",
			expectedGroup:            "",
			expectedError:            true,
		},
		{
			name:                     "Invalid scheduler group number",
			podName:                  "pod-5",
			replicaPerSchedulerGroup: "2",
			schedulerGroupNum:        "invalid",
			expectedGroup:            "",
			expectedError:            true,
		},
		{
			name:                     "Group index out of range",
			podName:                  "pod-7",
			replicaPerSchedulerGroup: "2", // (7 / 2) = group 3, which exceeds the group count
			schedulerGroupNum:        "3", // Only 3 groups available
			expectedGroup:            "",
			expectedError:            true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set environment variables
			os.Setenv("REPLICA_PER_SCHEDULER_GROUP", tt.replicaPerSchedulerGroup)
			os.Setenv("SCHEDULER_GROUP_NUM", tt.schedulerGroupNum)
			defer os.Unsetenv("REPLICA_PER_SCHEDULER_GROUP")
			defer os.Unsetenv("SCHEDULER_GROUP_NUM")

			// Call GetSchedulerGroup
			group, err := GetSchedulerGroup(tt.podName)

			// Assert the results
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedGroup, group)
			}
		})
	}
}

// TestGetIndexFromPodName tests the getIndexFromPodName function.
func TestGetIndexFromPodName(t *testing.T) {
	tests := []struct {
		name      string
		podName   string
		expected  int
		expectErr bool
	}{
		{
			name:      "Valid pod name",
			podName:   "pod-123",
			expected:  123,
			expectErr: false,
		},
		{
			name:      "Invalid pod name with no index",
			podName:   "pod",
			expected:  0,
			expectErr: true,
		},
		{
			name:      "Invalid pod name with non-numeric index",
			podName:   "pod-abc",
			expected:  0,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call getIndexFromPodName
			index, err := getIndexFromPodName(tt.podName)

			// Assert the results
			if tt.expectErr {
				assert.Error(t, err)
				assert.Equal(t, 0, index)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, index)
			}
		})
	}
}
