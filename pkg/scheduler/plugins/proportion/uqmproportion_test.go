package proportion

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	schedulingv1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/cmd/scheduler/app/options"
	"volcano.sh/volcano/pkg/scheduler/actions/allocate"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/cache"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/util"
)

type TestData struct {
	name      string
	podGroups []*schedulingv1.PodGroup
	pods      []*v1.Pod
	nodes     []*v1.Node
	queues    []*schedulingv1.Queue
}

var trueValue = true
var plugins = []conf.Tier{
	{
		Plugins: []conf.PluginOption{
			{
				Name:             UqmProportionPluginName,
				EnabledOverused:  &trueValue,
				EnabledNodeOrder: &trueValue,
				Arguments: framework.Arguments{
					UqmDefaultQueueKey: DefaultUqmQueue,
				},
			},
		},
	},
}

func TestEventHandlers(t *testing.T) {

	test := testData("Simulating event handler test")
	allocateAction := allocate.New()
	t.Run(test.name, func(t *testing.T) {
		ssn := createAndOpenSession(test, nil)
		allocateAction.Execute(ssn)

		if _, ok := ssn.Jobs["c1/pg1"].TaskStatusIndex[api.Binding]; !ok {
			t.Errorf("expected job c1/pg1 in binding state, tasksMap: %v", ssn.Jobs["c1/pg1"].TaskStatusIndex)
		}

		sessionPlugins := framework.GetRegisteredSessionPlugins(ssn)
		var p interface{} = sessionPlugins[UqmProportionPluginName]
		plugin := p.(*uqmProportionPlugin)
		opts := plugin.proportionPlugin.queueOpts
		for id := range opts {
			if 1 != opts[id].share {
				t.Errorf("share should be 1, got %f", opts[id].share)
			}
		}
	})
}

func TestBindingTask(t *testing.T) {

	test := testData("Simulating to bind the tasks")
	allocateAction := allocate.New()
	t.Run(test.name, func(t *testing.T) {
		ssn := createAndOpenSession(test, nil)
		allocateAction.Execute(ssn)
		tasksMap := ssn.Jobs["c1/pg1"].TaskStatusIndex[api.Binding]
		if len(tasksMap) != 1 {
			t.Errorf("job c1/pg1 should be in binding state, tasksMap: %v", ssn.Jobs["c1/pg1"].TaskStatusIndex)
		}
	})
}

func TestPendingTask(t *testing.T) {

	test := testData("Simulating to keep tasks Pending")
	test.nodes = []*v1.Node{
		util.BuildNode("n1", api.BuildResourceList("5", "5Gi"), make(map[string]string)),
	}
	allocateAction := allocate.New()
	t.Run(test.name, func(t *testing.T) {
		ssn := createAndOpenSession(test, nil)
		allocateAction.Execute(ssn)
		tasksMap := ssn.Jobs["c1/pg1"].TaskStatusIndex[api.Pending]
		if 1 != len(tasksMap) {
			t.Errorf("job c1/pg1 should be in Pending state, tasksMap: %v", ssn.Jobs["c1/pg1"].TaskStatusIndex)
		}
	})
}

func TestOverUsedFn(t *testing.T) {

	test := testData("testing overused function")
	test.pods = []*v1.Pod{
		createPod("p1", "pg1"),
		createPod("p2", "pg2"),
	}
	test.podGroups = []*schedulingv1.PodGroup{
		createPodGroup("pg1", "q1"),
		createPodGroup("pg2", "q1"),
	}
	allocateAction := allocate.New()

	t.Run(test.name, func(t *testing.T) {
		schedulerCache := createCache(test)
		ssn := createAndOpenSession(test, schedulerCache)
		allocateAction.Execute(ssn)

		tasksMap := ssn.Jobs["c1/pg1"].TaskStatusIndex[api.Binding]
		if 1 != len(tasksMap) {
			t.Errorf("job c1/pg1 should be in binding state, tasksMap: %v", ssn.Jobs["c1/pg1"].TaskStatusIndex)
		}
		tasksMap = ssn.Jobs["c1/pg2"].TaskStatusIndex[api.Pending]
		if 1 != len(tasksMap) {
			t.Errorf("job c1/pg2 should be in Pending state, tasksMap: %v", ssn.Jobs["c1/pg2"].TaskStatusIndex)
		}
	})
}

func TestSkipOverUsedFnForUqmQueue(t *testing.T) {

	test := testData("testing skip overused function")
	test.pods = []*v1.Pod{
		createPod("p1", "pg1"),
		createPod("p2", "pg2"),
	}
	test.podGroups = []*schedulingv1.PodGroup{
		createPodGroup("pg1", DefaultUqmQueue),
		createPodGroup("pg2", DefaultUqmQueue),
	}
	allocateAction := allocate.New()

	t.Run(test.name, func(t *testing.T) {
		schedulerCache := createCache(test)
		ssn := createAndOpenSession(test, schedulerCache)
		allocateAction.Execute(ssn)

		tasksMap := ssn.Jobs["c1/pg1"].TaskStatusIndex[api.Binding]
		if 1 != len(tasksMap) {
			t.Errorf("job c1/pg1 should be in Binding state, tasksMap: %v", ssn.Jobs["c1/pg1"].TaskStatusIndex)
		}

		tasksMap = ssn.Jobs["c1/pg2"].TaskStatusIndex[api.Binding]
		if 1 != len(tasksMap) {
			t.Errorf("job c1/pg2 should be in Binding state, tasksMap: %v", ssn.Jobs["c1/pg2"].TaskStatusIndex)
		}
	})
}

func createAndOpenSession(test *TestData, schedulerCache *cache.SchedulerCache) *framework.Session {
	if nil == schedulerCache {
		schedulerCache = createCache(test)
	}

	framework.RegisterPluginBuilder(UqmProportionPluginName, NewUqmProportionPlugin)
	ssn := framework.OpenSession(schedulerCache, plugins, nil)
	return ssn
}

func testData(name string) *TestData {
	return &TestData{
		name: name,
		podGroups: []*schedulingv1.PodGroup{
			createPodGroup("pg1", "q1"),
		},
		pods: []*v1.Pod{
			createPod("p1", "pg1"),
		},
		nodes: []*v1.Node{
			util.BuildNode("n1", api.BuildResourceList("25", "25Gi", []api.ScalarResource{{Name: "pods", Value: "2"}}...), make(map[string]string)),
		},

		queues: []*schedulingv1.Queue{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "c1",
				},
				Spec: schedulingv1.QueueSpec{
					Weight: 1,
				},
			},
		},
	}
}

func createPod(name string, pg string) *v1.Pod {
	return createPodWithResource(name, pg, api.BuildResourceList("10", "10G"))
}
func createPodWithResource(name string, pg string, r v1.ResourceList) *v1.Pod {
	return util.BuildPod("c1", name, "", v1.PodPending, r, pg, make(map[string]string), make(map[string]string))
}

func createPodGroup(name string, queue string) *schedulingv1.PodGroup {
	return &schedulingv1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "c1",
		},
		Spec: schedulingv1.PodGroupSpec{
			Queue: queue,
		},
		Status: schedulingv1.PodGroupStatus{
			Phase: schedulingv1.PodGroupInqueue,
		},
	}

}

func createCache(test *TestData) *cache.SchedulerCache {
	options.ServerOpts = &options.ServerOption{
		MinNodesToFind:             100,
		MinPercentageOfNodesToFind: 5,
		PercentageOfNodesToFind:    100,
	}
	options.ServerOpts.RegisterOptions()

	schedulerCache := cache.NewDefaultMockSchedulerCache("volcano")
	schedulerCache.Nodes = map[string]*api.NodeInfo{}
	schedulerCache.Jobs = map[api.JobID]*api.JobInfo{}
	schedulerCache.Queues = map[api.QueueID]*api.QueueInfo{}

	for _, node := range test.nodes {
		schedulerCache.AddOrUpdateNode(node)
	}
	for _, pod := range test.pods {
		schedulerCache.AddPod(pod)
	}

	for _, ss := range test.podGroups {
		schedulerCache.AddPodGroupV1beta1(ss)
		schedulerCache.AddQueueV1beta1(&schedulingv1.Queue{
			ObjectMeta: metav1.ObjectMeta{
				Name: ss.Spec.Queue,
			},
			Spec: schedulingv1.QueueSpec{
				Weight: 1,
				Capability: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU:    resource.MustParse("10"),
					v1.ResourceMemory: resource.MustParse("10G"),
					v1.ResourcePods:   resource.MustParse("1"),
				},
			},
		})
	}

	for _, q := range test.queues {
		schedulerCache.AddQueueV1beta1(q)
	}
	return schedulerCache
}
