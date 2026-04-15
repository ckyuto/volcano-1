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
	"k8s.io/klog/v2"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

type Action struct{}

func New() *Action {
	return &Action{}
}

func (pmptl *Action) Name() string {
	return "preemptall"
}

func (pmptl *Action) Initialize() {}

func (pmptl *Action) Execute(ssn *framework.Session) {
	klog.V(5).Infof("Enter Preempt All...")
	defer klog.V(5).Infof("Leaving Preempt All...")
	stmt := framework.NewStatement(ssn)
	for _, victim := range ssn.GetPreemptableTasks() {
		if err := stmt.Evict(victim.Clone(), "preemption via preempt all action"); err != nil {
			klog.Errorf("Failed to evict Task <%s/%s>: %v",
				victim.Namespace, victim.Name, err)
			continue
		}
	}
	stmt.Commit()
}

func (pmptl *Action) UnInitialize() {}
