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

package preemptall

import (
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	scheduling "volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/cmd/scheduler/app/options"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/uqm"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

func TestPreemptAll(t *testing.T) {
	plugins := map[string]framework.PluginBuilder{
		uqm.PluginName: uqm.New,
	}
	options.Default()
	uqmPreemtableLabels := map[string]string{
		"quota.linkedin.com/quota-preemptable":    "true",
		"quota.linkedin.com/quota-preemptable-at": "1234",
	}
	tests := []uthelper.TestCommonStruct{
		{
			Name: "Preempt all tasks",
			Pods: []*v1.Pod{
				util.BuildPod("c1", "preemptee1", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", uqmPreemtableLabels, make(map[string]string)),
				util.BuildPod("c1", "preemptee2", "n2", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", uqmPreemtableLabels, make(map[string]string)),
				util.BuildPod("c1", "preemptee3", "n3", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", uqmPreemtableLabels, make(map[string]string)),
			},
			ExpectEvictNum: 3,
			ExpectEvicted:  []string{"c1/preemptee1", "c1/preemptee2", "c1/preemptee3"},
		},
		{
			Name: "Preempt some tasks",
			Pods: []*v1.Pod{
				util.BuildPod("c1", "preemptee1", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", uqmPreemtableLabels, make(map[string]string)),
				util.BuildPod("c1", "preemptee2", "n2", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", uqmPreemtableLabels, make(map[string]string)),
				util.BuildPod("c1", "preemptee3", "n3", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
			},
			ExpectEvictNum: 2,
			ExpectEvicted:  []string{"c1/preemptee1", "c1/preemptee2"},
		},
		{
			Name: "Preempt no tasks",
			Pods: []*v1.Pod{
				util.BuildPod("c1", "preemptee1", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "preemptee2", "n2", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "preemptee3", "n3", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
			},
			ExpectEvictNum: 0,
			ExpectEvicted:  []string{},
		},
	}

	trueValue := true
	tiers := []conf.Tier{
		{
			Plugins: []conf.PluginOption{
				{
					Name:                    uqm.PluginName,
					EnabledPreemptable:      &trueValue,
					EnabledPredicate:        &trueValue,
					EnabledPreemptableTasks: &trueValue,
				},
			},
		}}

	actions := []framework.Action{New()}
	for i, test := range tests {
		test.Plugins = plugins
		t.Run(test.Name, func(t *testing.T) {
			ssn := test.RegisterSession(tiers, nil)
			for idx, pod := range test.Pods {
				jobID := api.JobID(fmt.Sprintf("job%d", idx))
				ssn.Jobs[jobID] = api.NewJobInfo(api.JobID(fmt.Sprintf("job%d-UUID", idx)), api.NewTaskInfo(pod))
				ssn.Jobs[jobID].AddTaskInfo(api.NewTaskInfo(pod))
				ssn.Jobs[jobID].PodGroup = &api.PodGroup{
					PodGroup: scheduling.PodGroup{
						ObjectMeta: metav1.ObjectMeta{
							Name: fmt.Sprintf("pg-job%d", idx),
						},
					},
				}
			}
			// This action is heavily dependent on the implementation at plugin level.
			// At the time of writing this test, uqm plugin is the only one that implements PreemptableTasksFn.
			uqm.New(framework.Arguments{}).OnSessionOpen(ssn)
			test.Run(actions)
			if err := test.CheckAll(i); err != nil {
				t.Fatal(err)
			}
		})
	}
}
