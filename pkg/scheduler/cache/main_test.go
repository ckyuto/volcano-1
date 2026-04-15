/*
Copyright 2023 The Volcano Authors.

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
	"stathat.com/c/consistent"
	"strconv"
	"testing"
)

var globalConsistent *consistent.Consistent
var SCHEDULER_GROUP_NUM int = 1000
var REPLICA_PER_SCHEDULER_GROUP int = 2

func TestMain(m *testing.M) {
	os.Setenv("SCHEDULER_GROUP_NUM", strconv.Itoa(SCHEDULER_GROUP_NUM))
	os.Setenv("REPLICA_PER_SCHEDULER_GROUP", strconv.Itoa(REPLICA_PER_SCHEDULER_GROUP))
	globalConsistent = consistent.New()

	// Add 1000 schedulers to the hash ring.
	// The chance of 2 entities having hashed scheduler is 0.1% which also becomes the failure % of the test.
	// Unfortunately, this is needed as the new function signature does not accpect Mock.
	for i := 0; i < SCHEDULER_GROUP_NUM; i++ {
		schedulerName := fmt.Sprintf("%s%d", schedulerGroupPrefix, i)
		globalConsistent.Add(schedulerName)
	}
	os.Exit(m.Run())
}
