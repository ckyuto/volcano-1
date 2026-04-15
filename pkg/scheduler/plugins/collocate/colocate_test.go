package collocate

import (
	"sort"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/cmd/scheduler/app/options"
	"volcano.sh/volcano/pkg/scheduler/actions/allocate"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/cache"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/binpack"
	"volcano.sh/volcano/pkg/scheduler/util"
	"volcano.sh/volcano/pkg/scheduler/util/assert"
)

func TestColocate_IB_NonGangJob(t *testing.T) {
	enabled := true
	schedulerCache := cache.NewDefaultMockSchedulerCache("volcano")
	schedulerCache.Queues = map[api.QueueID]*api.QueueInfo{
		"default": {
			UID:    "default",
			Name:   "default",
			Weight: 1,
		},
	}
	schedulerCache.AddPodGroupV1beta1(&schedulingv1beta1.PodGroup{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PodGroup",
			APIVersion: "scheduling.volcano.sh/v1beta1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pg",
			UID:       "pg",
			Namespace: "ns-1",
		},
		Spec: schedulingv1beta1.PodGroupSpec{
			Queue: "default",
		},
	})
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-1", "", "pod-1", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg",
			collocateAnnotation:            "required",
		}))
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-2", "", "pod-2", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg",
			collocateAnnotation:            "required",
		}))
	schedulerCache.AddOrUpdateNode(buildNode("node-1", "3000m", "4G", "5", map[string]string{
		"node.linkedin.com/pool": "pool-1",
		mzColocationGroup:        "1",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-2", "4000m", "4G", "5", map[string]string{
		"node.linkedin.com/pool": "pool-2",
		mzColocationGroup:        "2",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-3", "4000m", "4G", "5", map[string]string{
		"node.linkedin.com/pool": "pool-2",
		mzColocationGroup:        "1",
	}))
	schedulerCache.BindFlowChannel = make(chan *cache.BindContext, 5000)
	framework.RegisterPluginBuilder(ColocatePluginName, NewColocatePlugin)
	framework.RegisterPluginBuilder("binpack", binpack.New)
	option := options.NewServerOption()
	option.PercentageOfNodesToFind = 100
	option.RegisterOptions()
	ssn := framework.OpenSession(schedulerCache, []conf.Tier{
		{
			Plugins: []conf.PluginOption{
				{
					Name:             ColocatePluginName,
					EnabledPredicate: &enabled,
					EnabledNodeOrder: &enabled,
					Arguments: map[string]interface{}{
						mzScoreArgumentKey: 20.0,
						mzPluginName:       true,
						ibPluginName:       true,
					},
				},
				{
					Name:             "binpack",
					EnabledNodeOrder: &enabled,
					Arguments:        map[string]interface{}{},
				},
			},
		},
	},
		[]conf.Configuration{
			{
				Name: "allocate",
			},
		},
	)
	allocate := allocate.New()
	allocate.Execute(ssn)
	klog.Infof("Task1 : %s Task2 : %s", ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-1"].NodeName, ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-2"].NodeName)
	assert.Assert(ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-1"].NodeName != "node-1", "Task should not be scheduled on node-1")
	assert.Assert(ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-2"].NodeName != "node-1", "Task should not be scheduled on node-1")
}

func TestColocate_IB_GangJob(t *testing.T) {
	enabled := true
	schedulerCache := cache.NewDefaultMockSchedulerCache("volcano")
	schedulerCache.Queues = map[api.QueueID]*api.QueueInfo{
		"default": {
			UID:    "default",
			Name:   "default",
			Weight: 1,
		},
	}
	schedulerCache.AddPodGroupV1beta1(&schedulingv1beta1.PodGroup{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PodGroup",
			APIVersion: "scheduling.volcano.sh/v1beta1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pg",
			UID:       "pg",
			Namespace: "ns-1",
		},
		Spec: schedulingv1beta1.PodGroupSpec{
			Queue:     "default",
			MinMember: 2,
		},
	})
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-1", "", "pod-1", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg",
			collocateAnnotation:            "required",
		}))
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-2", "", "pod-2", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg",
			collocateAnnotation:            "required",
		}))
	schedulerCache.AddOrUpdateNode(buildNode("node-1", "3000m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-1",
		mzColocationGroup: "1",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-2", "4000m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "2",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-3", "4000m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "1",
	}))
	schedulerCache.BindFlowChannel = make(chan *cache.BindContext, 5000)
	framework.RegisterPluginBuilder(ColocatePluginName, NewColocatePlugin)
	framework.RegisterPluginBuilder("binpack", binpack.New)
	option := options.NewServerOption()
	option.PercentageOfNodesToFind = 100
	option.RegisterOptions()
	ssn := framework.OpenSession(schedulerCache, []conf.Tier{
		{
			Plugins: []conf.PluginOption{
				{
					Name:             ColocatePluginName,
					EnabledPredicate: &enabled,
					EnabledNodeOrder: &enabled,
					Arguments: map[string]interface{}{
						mzScoreArgumentKey: 20.0,
						mzPluginName:       true,
						ibPluginName:       true,
					},
				},
				{
					Name:             "binpack",
					EnabledNodeOrder: &enabled,
					Arguments:        map[string]interface{}{},
				},
			},
		},
	},
		[]conf.Configuration{
			{
				Name: "allocate",
			},
		},
	)
	allocate := allocate.New()
	allocate.Execute(ssn)
	klog.Infof("Task1 : %s Task2 : %s", ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-1"].NodeName, ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-2"].NodeName)
	assert.Assert(ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-1"].NodeName != "node-1", "Task should not be scheduled on node-1")
	assert.Assert(ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-2"].NodeName != "node-1", "Task should not be scheduled on node-1")
}

func TestColocate_Non_IB_GangJob(t *testing.T) {
	enabled := true
	schedulerCache := cache.NewDefaultMockSchedulerCache("volcano")
	schedulerCache.Queues = map[api.QueueID]*api.QueueInfo{
		"default": {
			UID:    "default",
			Name:   "default",
			Weight: 1,
		},
	}
	schedulerCache.AddPodGroupV1beta1(&schedulingv1beta1.PodGroup{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PodGroup",
			APIVersion: "scheduling.volcano.sh/v1beta1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pg",
			UID:       "pg",
			Namespace: "ns-1",
		},
		Spec: schedulingv1beta1.PodGroupSpec{
			Queue:     "default",
			MinMember: 2,
		},
	})
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-1", "", "pod-1", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg",
		}))
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-2", "", "pod-2", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg",
		}))
	schedulerCache.AddOrUpdateNode(buildNode("node-1", "3000m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-1",
		mzColocationGroup: "1",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-2", "4000m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "2",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-3", "4000m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "1",
	}))
	schedulerCache.BindFlowChannel = make(chan *cache.BindContext, 5000)
	framework.RegisterPluginBuilder(ColocatePluginName, NewColocatePlugin)
	framework.RegisterPluginBuilder("binpack", binpack.New)
	option := options.NewServerOption()
	option.PercentageOfNodesToFind = 100
	option.RegisterOptions()
	ssn := framework.OpenSession(schedulerCache, []conf.Tier{
		{
			Plugins: []conf.PluginOption{
				{
					Name:             ColocatePluginName,
					EnabledPredicate: &enabled,
					EnabledNodeOrder: &enabled,
					Arguments: map[string]interface{}{
						mzScoreArgumentKey: 20.0,
						mzPluginName:       true,
						ibPluginName:       true,
					},
				},
				{
					Name:             "binpack",
					EnabledNodeOrder: &enabled,
					Arguments:        map[string]interface{}{},
				},
			},
		},
	},
		[]conf.Configuration{
			{
				Name: "allocate",
			},
		},
	)
	allocate := allocate.New()
	allocate.Execute(ssn)
	klog.Infof("Task1 : %s Task2 : %s", ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-1"].NodeName, ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-2"].NodeName)
	assert.Assert(ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-1"].NodeName != "node-2", "Task should not be scheduled on node-2")
	assert.Assert(ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-2"].NodeName != "node-2", "Task should not be scheduled on node-2")
}

func TestColocate_BestNodeGroupScore_IB_NonGangJob(t *testing.T) {
	enabled := true
	schedulerCache := cache.NewDefaultMockSchedulerCache("volcano")
	schedulerCache.Queues = map[api.QueueID]*api.QueueInfo{
		"default": {
			UID:    "default",
			Name:   "default",
			Weight: 1,
		},
	}
	schedulerCache.AddPodGroupV1beta1(&schedulingv1beta1.PodGroup{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PodGroup",
			APIVersion: "scheduling.volcano.sh/v1beta1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pg",
			UID:       "pg",
			Namespace: "ns-1",
		},
		Spec: schedulingv1beta1.PodGroupSpec{
			Queue: "default",
		},
	})
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-1", "", "pod-1", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg",
			collocateAnnotation:            "required",
		}))
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-2", "", "pod-2", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg",
			collocateAnnotation:            "required",
		}))
	schedulerCache.AddOrUpdateNode(buildNode("node-1", "3000m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-1",
		mzColocationGroup: "1",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-2", "3000m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-1",
		mzColocationGroup: "2",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-3", "3500m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "1",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-4", "4000m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "2",
	}))
	schedulerCache.BindFlowChannel = make(chan *cache.BindContext, 5000)
	framework.RegisterPluginBuilder(ColocatePluginName, NewColocatePlugin)
	framework.RegisterPluginBuilder("binpack", binpack.New)
	option := options.NewServerOption()
	option.PercentageOfNodesToFind = 100
	option.RegisterOptions()
	ssn := framework.OpenSession(schedulerCache, []conf.Tier{
		{
			Plugins: []conf.PluginOption{
				{
					Name:             ColocatePluginName,
					EnabledPredicate: &enabled,
					EnabledNodeOrder: &enabled,
					Arguments: map[string]interface{}{
						mzScoreArgumentKey: 20.0,
						mzPluginName:       true,
						ibPluginName:       true,
					},
				},
				{
					Name:             "binpack",
					EnabledNodeOrder: &enabled,
					Arguments:        map[string]interface{}{},
				},
			},
		},
	},
		[]conf.Configuration{
			{
				Name: "allocate",
			},
		},
	)
	allocate := allocate.New()
	allocate.Execute(ssn)
	klog.Infof("Task1 : %s Task2 : %s", ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-1"].NodeName, ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-2"].NodeName)
	assert.Assert(ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-1"].NodeName != "node-3", "Task should not be scheduled on node-3 and node-4")
	assert.Assert(ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-2"].NodeName != "node-3", "Task should not be scheduled on node-3 and node-4")
	assert.Assert(ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-2"].NodeName != "node-4", "Task should not be scheduled on node-3 and node-4")
	assert.Assert(ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-2"].NodeName != "node-4", "Task should not be scheduled on node-3 and node-4")
}

func TestColocate_BestNodeGroupScore_IB_GangJob(t *testing.T) {
	enabled := true
	schedulerCache := cache.NewDefaultMockSchedulerCache("volcano")
	schedulerCache.Queues = map[api.QueueID]*api.QueueInfo{
		"default": {
			UID:    "default",
			Name:   "default",
			Weight: 1,
		},
	}
	schedulerCache.AddPodGroupV1beta1(&schedulingv1beta1.PodGroup{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PodGroup",
			APIVersion: "scheduling.volcano.sh/v1beta1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pg",
			UID:       "pg",
			Namespace: "ns-1",
		},
		Spec: schedulingv1beta1.PodGroupSpec{
			Queue:     "default",
			MinMember: 2,
		},
	})
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-1", "", "pod-1", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg",
			collocateAnnotation:            "required",
		}))
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-2", "", "pod-2", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg",
			collocateAnnotation:            "required",
		}))
	schedulerCache.AddOrUpdateNode(buildNode("node-1", "3000m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-1",
		mzColocationGroup: "1",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-2", "3000m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-1",
		mzColocationGroup: "2",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-3", "3500m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "1",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-4", "4000m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "2",
	}))
	schedulerCache.BindFlowChannel = make(chan *cache.BindContext, 5000)
	framework.RegisterPluginBuilder(ColocatePluginName, NewColocatePlugin)
	framework.RegisterPluginBuilder("binpack", binpack.New)
	option := options.NewServerOption()
	option.PercentageOfNodesToFind = 100
	option.RegisterOptions()
	ssn := framework.OpenSession(schedulerCache, []conf.Tier{
		{
			Plugins: []conf.PluginOption{
				{
					Name:             ColocatePluginName,
					EnabledPredicate: &enabled,
					EnabledNodeOrder: &enabled,
					Arguments: map[string]interface{}{
						mzScoreArgumentKey: 20.0,
						mzPluginName:       true,
						ibPluginName:       true,
					},
				},
				{
					Name:             "binpack",
					EnabledNodeOrder: &enabled,
					Arguments:        map[string]interface{}{},
				},
			},
		},
	},
		[]conf.Configuration{
			{
				Name: "allocate",
			},
		},
	)
	allocate := allocate.New()
	allocate.Execute(ssn)
	klog.Infof("Task1 : %s Task2 : %s", ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-1"].NodeName, ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-2"].NodeName)
	assert.Assert(ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-1"].NodeName != "node-3", "Task should not be scheduled on node-3 and node-4")
	assert.Assert(ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-2"].NodeName != "node-3", "Task should not be scheduled on node-3 and node-4")
	assert.Assert(ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-2"].NodeName != "node-4", "Task should not be scheduled on node-3 and node-4")
	assert.Assert(ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-2"].NodeName != "node-4", "Task should not be scheduled on node-3 and node-4")
}

func TestColocate_BestNodeGroupScore_Non_IB_GangJob(t *testing.T) {
	enabled := true
	schedulerCache := cache.NewDefaultMockSchedulerCache("volcano")
	schedulerCache.Queues = map[api.QueueID]*api.QueueInfo{
		"default": {
			UID:    "default",
			Name:   "default",
			Weight: 1,
		},
	}
	schedulerCache.AddPodGroupV1beta1(&schedulingv1beta1.PodGroup{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PodGroup",
			APIVersion: "scheduling.volcano.sh/v1beta1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pg",
			UID:       "pg",
			Namespace: "ns-1",
		},
		Spec: schedulingv1beta1.PodGroupSpec{
			Queue:     "default",
			MinMember: 2,
		},
	})
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-1", "", "pod-1", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg",
		}))
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-2", "", "pod-2", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg",
		}))
	schedulerCache.AddOrUpdateNode(buildNode("node-1", "3000m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-1",
		mzColocationGroup: "1",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-2", "3000m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-1",
		mzColocationGroup: "2",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-3", "3000m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "1",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-4", "4000m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "2",
	}))
	schedulerCache.BindFlowChannel = make(chan *cache.BindContext, 5000)
	framework.RegisterPluginBuilder(ColocatePluginName, NewColocatePlugin)
	framework.RegisterPluginBuilder("binpack", binpack.New)
	option := options.NewServerOption()
	option.PercentageOfNodesToFind = 100
	option.RegisterOptions()
	ssn := framework.OpenSession(schedulerCache, []conf.Tier{
		{
			Plugins: []conf.PluginOption{
				{
					Name:             ColocatePluginName,
					EnabledPredicate: &enabled,
					EnabledNodeOrder: &enabled,
					Arguments: map[string]interface{}{
						mzScoreArgumentKey: 20.0,
						mzPluginName:       true,
						ibPluginName:       true,
					},
				},
				{
					Name:             "binpack",
					EnabledNodeOrder: &enabled,
					Arguments:        map[string]interface{}{},
				},
			},
		},
	},
		[]conf.Configuration{
			{
				Name: "allocate",
			},
		},
	)
	allocate := allocate.New()
	allocate.Execute(ssn)
	klog.Infof("Task1 : %s Task2 : %s", ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-1"].NodeName, ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-2"].NodeName)
	assert.Assert(ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-1"].NodeName != "node-2", "Task should not be scheduled on node-2 and node-4")
	assert.Assert(ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-2"].NodeName != "node-2", "Task should not be scheduled on node-2 and node-4")
	assert.Assert(ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-2"].NodeName != "node-4", "Task should not be scheduled on node-2 and node-4")
	assert.Assert(ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-2"].NodeName != "node-4", "Task should not be scheduled on node-2 and node-4")
}

func TestColocate_PartialColocation_IB_NonGangJob(t *testing.T) {
	enabled := true
	schedulerCache := cache.NewDefaultMockSchedulerCache("volcano")
	schedulerCache.Queues = map[api.QueueID]*api.QueueInfo{
		"default": {
			UID:    "default",
			Name:   "default",
			Weight: 1,
		},
	}
	schedulerCache.AddPodGroupV1beta1(&schedulingv1beta1.PodGroup{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PodGroup",
			APIVersion: "scheduling.volcano.sh/v1beta1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pg",
			UID:       "pg",
			Namespace: "ns-1",
		},
		Spec: schedulingv1beta1.PodGroupSpec{
			Queue: "default",
		},
	})
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-1", "", "pod-1", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg",
			collocateAnnotation:            "required",
		}))
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-2", "", "pod-2", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg",
		}))

	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-3", "", "pod-3", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg",
			collocateAnnotation:            "required",
		}))
	schedulerCache.AddOrUpdateNode(buildNode("node-1", "3000m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-1",
		mzColocationGroup: "1",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-2", "3500m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "2",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-3", "3500m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "1",
	}))
	schedulerCache.BindFlowChannel = make(chan *cache.BindContext, 5000)
	framework.RegisterPluginBuilder(ColocatePluginName, NewColocatePlugin)
	framework.RegisterPluginBuilder("binpack", binpack.New)
	option := options.NewServerOption()
	option.PercentageOfNodesToFind = 100
	option.RegisterOptions()
	ssn := framework.OpenSession(schedulerCache, []conf.Tier{
		{
			Plugins: []conf.PluginOption{
				{
					Name:             ColocatePluginName,
					EnabledPredicate: &enabled,
					EnabledNodeOrder: &enabled,
					Arguments: map[string]interface{}{
						mzScoreArgumentKey: 20.0,
						mzPluginName:       true,
						ibPluginName:       true,
					},
				},
				{
					Name:             "binpack",
					EnabledNodeOrder: &enabled,
					Arguments:        map[string]interface{}{},
				},
			},
		},
	},
		[]conf.Configuration{
			{
				Name: "allocate",
			},
		},
	)
	allocate := allocate.New()
	allocate.Execute(ssn)
	klog.Infof("pod-1 : %s; pod-2 : %s; pod-3 : %s", ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-1"].NodeName, ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-2"].NodeName, ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-3"].NodeName)
	assert.Assert(ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-1"].NodeName != "node-1", "Task should not be scheduled on node-1")
	assert.Assert(ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-2"].NodeName == "node-1", "Task should be scheduled on node-1")
	assert.Assert(ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-3"].NodeName != "node-1", "Task should not be scheduled on node-1")
}

func TestColocate_PartialColocation_IB_GangJob(t *testing.T) {
	enabled := true
	schedulerCache := cache.NewDefaultMockSchedulerCache("volcano")
	schedulerCache.Queues = map[api.QueueID]*api.QueueInfo{
		"default": {
			UID:    "default",
			Name:   "default",
			Weight: 1,
		},
	}
	schedulerCache.AddPodGroupV1beta1(&schedulingv1beta1.PodGroup{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PodGroup",
			APIVersion: "scheduling.volcano.sh/v1beta1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pg",
			UID:       "pg",
			Namespace: "ns-1",
		},
		Spec: schedulingv1beta1.PodGroupSpec{
			Queue:     "default",
			MinMember: 3,
		},
	})
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-1", "", "pod-1", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg",
			collocateAnnotation:            "required",
		}))
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-2", "", "pod-2", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg",
		}))

	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-3", "", "pod-3", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg",
			collocateAnnotation:            "required",
		}))
	schedulerCache.AddOrUpdateNode(buildNode("node-1", "3000m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-1",
		mzColocationGroup: "1",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-2", "3500m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "2",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-3", "3500m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "1",
	}))
	schedulerCache.BindFlowChannel = make(chan *cache.BindContext, 5000)
	framework.RegisterPluginBuilder(ColocatePluginName, NewColocatePlugin)
	framework.RegisterPluginBuilder("binpack", binpack.New)
	option := options.NewServerOption()
	option.PercentageOfNodesToFind = 100
	option.RegisterOptions()
	ssn := framework.OpenSession(schedulerCache, []conf.Tier{
		{
			Plugins: []conf.PluginOption{
				{
					Name:             ColocatePluginName,
					EnabledPredicate: &enabled,
					EnabledNodeOrder: &enabled,
					Arguments: map[string]interface{}{
						mzScoreArgumentKey: 20.0,
						mzPluginName:       true,
						ibPluginName:       true,
					},
				},
				{
					Name:             "binpack",
					EnabledNodeOrder: &enabled,
					Arguments:        map[string]interface{}{},
				},
			},
		},
	},
		[]conf.Configuration{
			{
				Name: "allocate",
			},
		},
	)
	allocate := allocate.New()
	allocate.Execute(ssn)
	klog.Infof("pod-1 : %s; pod-2 : %s; pod-3 : %s", ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-1"].NodeName, ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-2"].NodeName, ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-3"].NodeName)
	assert.Assert(ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-1"].NodeName != "node-1", "Task should not be scheduled on node-1")
	assert.Assert(ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-2"].NodeName == "node-1", "Task should be scheduled on node-1")
	assert.Assert(ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-3"].NodeName != "node-1", "Task should not be scheduled on node-1")
}

func TestColocate_PartialColocation_Non_IB_GangJob(t *testing.T) {
	enabled := true
	schedulerCache := cache.NewDefaultMockSchedulerCache("volcano")
	schedulerCache.Queues = map[api.QueueID]*api.QueueInfo{
		"default": {
			UID:    "default",
			Name:   "default",
			Weight: 1,
		},
	}
	schedulerCache.AddPodGroupV1beta1(&schedulingv1beta1.PodGroup{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PodGroup",
			APIVersion: "scheduling.volcano.sh/v1beta1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pg",
			UID:       "pg",
			Namespace: "ns-1",
		},
		Spec: schedulingv1beta1.PodGroupSpec{
			Queue:     "default",
			MinMember: 3,
		},
	})
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-1", "", "pod-1", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg",
		}))
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-2", "", "pod-2", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg",
		}))

	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-3", "", "pod-3", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg",
		}))
	schedulerCache.AddOrUpdateNode(buildNode("node-1", "3000m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-1",
		mzColocationGroup: "1",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-2", "3500m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "2",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-3", "3500m", "4G", "5", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "1",
	}))
	mzNodes := make(map[string]string)
	mzNodes["node-1"] = "1"
	mzNodes["node-2"] = "2"
	mzNodes["node-3"] = "1"
	schedulerCache.BindFlowChannel = make(chan *cache.BindContext, 5000)
	framework.RegisterPluginBuilder(ColocatePluginName, NewColocatePlugin)
	framework.RegisterPluginBuilder("binpack", binpack.New)
	option := options.NewServerOption()
	option.PercentageOfNodesToFind = 100
	option.RegisterOptions()
	ssn := framework.OpenSession(schedulerCache, []conf.Tier{
		{
			Plugins: []conf.PluginOption{
				{
					Name:             ColocatePluginName,
					EnabledPredicate: &enabled,
					EnabledNodeOrder: &enabled,
					Arguments: map[string]interface{}{
						mzScoreArgumentKey: 20.0,
						mzPluginName:       true,
						ibPluginName:       true,
					},
				},
				{
					Name:             "binpack",
					EnabledNodeOrder: &enabled,
					Arguments:        map[string]interface{}{},
				},
			},
		},
	},
		[]conf.Configuration{
			{
				Name: "allocate",
			},
		},
	)
	allocate := allocate.New()
	allocate.Execute(ssn)
	klog.Infof("pod-1 : %s; pod-2 : %s; pod-3 : %s", ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-1"].NodeName, ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-2"].NodeName, ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-3"].NodeName)

	mzTasks := map[string]int8{
		"1": 0,
		"2": 0,
	}
	for _, task := range ssn.Jobs[api.JobID("ns-1/pg")].Tasks {
		mzTasks[mzNodes[task.NodeName]]++
	}
	assert.Assert(mzTasks["1"] == 2, "2 Task should be scheduled in MZ-1")
	assert.Assert(mzTasks["2"] == 1, "1 Task should be scheduled in MZ-2")
}

func TestColocate_NodePoolBinpacking_IB_NonGangJob(t *testing.T) {
	enabled := true
	schedulerCache := cache.NewDefaultMockSchedulerCache("volcano")
	schedulerCache.Queues = map[api.QueueID]*api.QueueInfo{
		"default": {
			UID:    "default",
			Name:   "default",
			Weight: 1,
		},
	}
	schedulerCache.AddPodGroupV1beta1(&schedulingv1beta1.PodGroup{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PodGroup",
			APIVersion: "scheduling.volcano.sh/v1beta1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pg-1",
			UID:       "pg-1",
			Namespace: "ns-1",
		},
		Spec: schedulingv1beta1.PodGroupSpec{
			Queue: "default",
		},
	})
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-2", "", "pod-2", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg-1",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg-1",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg-1",
			collocateAnnotation:            "required",
		}))
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-1", "", "pod-1", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg-1",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg-1",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg-1",
			collocateAnnotation:            "required",
		}))
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-3", "", "pod-3", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg-1",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg-1",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg-1",
			collocateAnnotation:            "required",
		}))

	schedulerCache.AddPodGroupV1beta1(&schedulingv1beta1.PodGroup{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PodGroup",
			APIVersion: "scheduling.volcano.sh/v1beta1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pg-2",
			UID:       "pg-2",
			Namespace: "ns-2",
		},
		Spec: schedulingv1beta1.PodGroupSpec{
			Queue: "default",
		},
	})
	schedulerCache.AddPod(buildPod("ns-2", "mjuluri-pod-1", "", "pod-1", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg-2",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg-2",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg-2",
			collocateAnnotation:            "required",
		}))
	schedulerCache.AddPod(buildPod("ns-2", "mjuluri-pod-2", "", "pod-2", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg-2",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg-2",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg-2",
			collocateAnnotation:            "required",
		}))
	schedulerCache.AddOrUpdateNode(buildNode("node-1", "3500m", "4G", "7", map[string]string{
		ibColocationGroup: "pool-1",
		mzColocationGroup: "1",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-2", "3500m", "4G", "7", map[string]string{
		ibColocationGroup: "pool-1",
		mzColocationGroup: "2",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-3", "3500m", "4G", "6", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "1",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-4", "3500m", "4G", "7", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "2",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-5", "3500m", "4G", "7", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "1",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-6", "3500m", "4G", "7", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "2",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-7", "3500m", "4G", "7", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "1",
	}))
	poolNodes := make(map[string]string)
	poolNodes["node-1"] = "pool-1"
	poolNodes["node-2"] = "pool-1"
	poolNodes["node-3"] = "pool-2"
	poolNodes["node-4"] = "pool-2"
	poolNodes["node-5"] = "pool-2"
	poolNodes["node-6"] = "pool-2"
	poolNodes["node-7"] = "pool-2"
	mzNodes := make(map[string]string)
	mzNodes["node-1"] = "1"
	mzNodes["node-2"] = "2"
	mzNodes["node-3"] = "1"
	mzNodes["node-4"] = "2"
	mzNodes["node-5"] = "1"
	mzNodes["node-6"] = "2"
	mzNodes["node-7"] = "1"
	schedulerCache.BindFlowChannel = make(chan *cache.BindContext, 5000)
	framework.RegisterPluginBuilder(ColocatePluginName, NewColocatePlugin)
	framework.RegisterPluginBuilder("binpack", binpack.New)
	option := options.NewServerOption()
	option.PercentageOfNodesToFind = 100
	option.RegisterOptions()
	ssn := framework.OpenSession(schedulerCache, []conf.Tier{
		{
			Plugins: []conf.PluginOption{
				{
					Name:             ColocatePluginName,
					EnabledPredicate: &enabled,
					EnabledNodeOrder: &enabled,
					Arguments: map[string]interface{}{
						mzScoreArgumentKey: 20.0,
						mzPluginName:       true,
						ibPluginName:       true,
					},
				},
				{
					Name:             "binpack",
					EnabledNodeOrder: &enabled,
					Arguments:        map[string]interface{}{},
				},
			},
		},
	},
		[]conf.Configuration{
			{
				Name: "allocate",
			},
		},
	)
	allocate := allocate.New()
	allocate.Execute(ssn)
	klog.Infof("pg-1/pod-1 : %s; pg-1/pod-2 : %s; pg-1/pod-3 : %s", ssn.Jobs[api.JobID("ns-1/pg-1")].Tasks["pod-1"].NodeName, ssn.Jobs[api.JobID("ns-1/pg-1")].Tasks["pod-2"].NodeName, ssn.Jobs[api.JobID("ns-1/pg-1")].Tasks["pod-3"].NodeName)
	klog.Infof("pg-2/pod-1 : %s; pg-2/pod-2 : %s", ssn.Jobs[api.JobID("ns-2/pg-2")].Tasks["pod-1"].NodeName, ssn.Jobs[api.JobID("ns-2/pg-2")].Tasks["pod-2"].NodeName)
	assert.Assert(poolNodes[ssn.Jobs[api.JobID("ns-1/pg-1")].Tasks["pod-1"].NodeName] == "pool-2", "Task should be scheduled on pool-2")
	assert.Assert(poolNodes[ssn.Jobs[api.JobID("ns-1/pg-1")].Tasks["pod-2"].NodeName] == "pool-2", "Task should be scheduled on pool-2")
	assert.Assert(poolNodes[ssn.Jobs[api.JobID("ns-1/pg-1")].Tasks["pod-3"].NodeName] == "pool-2", "Task should be scheduled on pool-2")

	assert.Assert(poolNodes[ssn.Jobs[api.JobID("ns-2/pg-2")].Tasks["pod-1"].NodeName] == "pool-1", "Task should be scheduled on pool-1")
	assert.Assert(poolNodes[ssn.Jobs[api.JobID("ns-2/pg-2")].Tasks["pod-2"].NodeName] == "pool-1", "Task should be scheduled on pool-1")
}

func TestColocate_NodePoolBinpacking_IB_GangJob(t *testing.T) {
	enabled := true
	schedulerCache := cache.NewDefaultMockSchedulerCache("volcano")
	schedulerCache.Queues = map[api.QueueID]*api.QueueInfo{
		"default": {
			UID:    "default",
			Name:   "default",
			Weight: 1,
		},
	}
	schedulerCache.AddPodGroupV1beta1(&schedulingv1beta1.PodGroup{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PodGroup",
			APIVersion: "scheduling.volcano.sh/v1beta1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pg-1",
			UID:       "pg-1",
			Namespace: "ns-1",
		},
		Spec: schedulingv1beta1.PodGroupSpec{
			Queue:     "default",
			MinMember: 3,
		},
	})
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-2", "", "pod-2", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg-1",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg-1",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg-1",
			collocateAnnotation:            "required",
		}))
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-1", "", "pod-1", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg-1",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg-1",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg-1",
			collocateAnnotation:            "required",
		}))
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-3", "", "pod-3", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg-1",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg-1",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg-1",
			collocateAnnotation:            "required",
		}))

	schedulerCache.AddPodGroupV1beta1(&schedulingv1beta1.PodGroup{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PodGroup",
			APIVersion: "scheduling.volcano.sh/v1beta1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pg-2",
			UID:       "pg-2",
			Namespace: "ns-2",
		},
		Spec: schedulingv1beta1.PodGroupSpec{
			Queue:     "default",
			MinMember: 2,
		},
	})
	schedulerCache.AddPod(buildPod("ns-2", "mjuluri-pod-1", "", "pod-1", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg-2",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg-2",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg-2",
			collocateAnnotation:            "required",
		}))
	schedulerCache.AddPod(buildPod("ns-2", "mjuluri-pod-2", "", "pod-2", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg-2",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg-2",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg-2",
			collocateAnnotation:            "required",
		}))
	schedulerCache.AddOrUpdateNode(buildNode("node-1", "3500m", "4G", "7", map[string]string{
		ibColocationGroup: "pool-1",
		mzColocationGroup: "1",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-2", "3500m", "4G", "7", map[string]string{
		ibColocationGroup: "pool-1",
		mzColocationGroup: "2",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-3", "3500m", "4G", "6", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "1",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-4", "3500m", "4G", "7", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "2",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-5", "3500m", "4G", "7", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "1",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-6", "3500m", "4G", "7", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "2",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-7", "3500m", "4G", "7", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "1",
	}))
	poolNodes := make(map[string]string)
	poolNodes["node-1"] = "pool-1"
	poolNodes["node-2"] = "pool-1"
	poolNodes["node-3"] = "pool-2"
	poolNodes["node-4"] = "pool-2"
	poolNodes["node-5"] = "pool-2"
	poolNodes["node-6"] = "pool-2"
	poolNodes["node-7"] = "pool-2"
	mzNodes := make(map[string]string)
	mzNodes["node-1"] = "1"
	mzNodes["node-2"] = "2"
	mzNodes["node-3"] = "1"
	mzNodes["node-4"] = "2"
	mzNodes["node-5"] = "1"
	mzNodes["node-6"] = "2"
	mzNodes["node-7"] = "1"
	schedulerCache.BindFlowChannel = make(chan *cache.BindContext, 5000)
	framework.RegisterPluginBuilder(ColocatePluginName, NewColocatePlugin)
	framework.RegisterPluginBuilder("binpack", binpack.New)
	option := options.NewServerOption()
	option.PercentageOfNodesToFind = 100
	option.RegisterOptions()
	ssn := framework.OpenSession(schedulerCache, []conf.Tier{
		{
			Plugins: []conf.PluginOption{
				{
					Name:             ColocatePluginName,
					EnabledPredicate: &enabled,
					EnabledNodeOrder: &enabled,
					Arguments: map[string]interface{}{
						mzScoreArgumentKey: 20.0,
						mzPluginName:       true,
						ibPluginName:       true,
					},
				},
				{
					Name:             "binpack",
					EnabledNodeOrder: &enabled,
					Arguments:        map[string]interface{}{},
				},
			},
		},
	},
		[]conf.Configuration{
			{
				Name: "allocate",
			},
		},
	)
	allocate := allocate.New()
	allocate.Execute(ssn)
	klog.Infof("pg-1/pod-1 : %s; pg-1/pod-2 : %s; pg-1/pod-3 : %s", ssn.Jobs[api.JobID("ns-1/pg-1")].Tasks["pod-1"].NodeName, ssn.Jobs[api.JobID("ns-1/pg-1")].Tasks["pod-2"].NodeName, ssn.Jobs[api.JobID("ns-1/pg-1")].Tasks["pod-3"].NodeName)
	klog.Infof("pg-2/pod-1 : %s; pg-2/pod-2 : %s", ssn.Jobs[api.JobID("ns-2/pg-2")].Tasks["pod-1"].NodeName, ssn.Jobs[api.JobID("ns-2/pg-2")].Tasks["pod-2"].NodeName)
	assert.Assert(poolNodes[ssn.Jobs[api.JobID("ns-1/pg-1")].Tasks["pod-1"].NodeName] == "pool-2", "Task should be scheduled on pool-2")
	assert.Assert(poolNodes[ssn.Jobs[api.JobID("ns-1/pg-1")].Tasks["pod-2"].NodeName] == "pool-2", "Task should be scheduled on pool-2")
	assert.Assert(poolNodes[ssn.Jobs[api.JobID("ns-1/pg-1")].Tasks["pod-3"].NodeName] == "pool-2", "Task should be scheduled on pool-2")

	assert.Assert(poolNodes[ssn.Jobs[api.JobID("ns-2/pg-2")].Tasks["pod-1"].NodeName] == "pool-1", "Task should be scheduled on pool-1")
	assert.Assert(poolNodes[ssn.Jobs[api.JobID("ns-2/pg-2")].Tasks["pod-2"].NodeName] == "pool-1", "Task should be scheduled on pool-1")
}

func TestColocate_NodePoolBinpacking_Non_IB_GangJob(t *testing.T) {
	enabled := true
	schedulerCache := cache.NewDefaultMockSchedulerCache("volcano")
	schedulerCache.Queues = map[api.QueueID]*api.QueueInfo{
		"default": {
			UID:    "default",
			Name:   "default",
			Weight: 1,
		},
	}
	schedulerCache.AddPodGroupV1beta1(&schedulingv1beta1.PodGroup{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PodGroup",
			APIVersion: "scheduling.volcano.sh/v1beta1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pg-1",
			UID:       "pg-1",
			Namespace: "ns-1",
		},
		Spec: schedulingv1beta1.PodGroupSpec{
			Queue:     "default",
			MinMember: 3,
		},
	})
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-2", "", "pod-2", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg-1",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg-1",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg-1",
		}))
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-1", "", "pod-1", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg-1",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg-1",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg-1",
		}))
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-3", "", "pod-3", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg-1",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg-1",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg-1",
		}))

	schedulerCache.AddPodGroupV1beta1(&schedulingv1beta1.PodGroup{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PodGroup",
			APIVersion: "scheduling.volcano.sh/v1beta1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pg-2",
			UID:       "pg-2",
			Namespace: "ns-2",
		},
		Spec: schedulingv1beta1.PodGroupSpec{
			Queue:     "default",
			MinMember: 2,
		},
	})
	schedulerCache.AddPod(buildPod("ns-2", "mjuluri-pod-1", "", "pod-1", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg-2",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg-2",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg-2",
		}))
	schedulerCache.AddPod(buildPod("ns-2", "mjuluri-pod-2", "", "pod-2", v1.PodPending, buildResourceList("2000m", "2G", "2"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg-2",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg-2",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg-2",
		}))
	schedulerCache.AddOrUpdateNode(buildNode("node-1", "4000m", "4G", "7", map[string]string{
		ibColocationGroup: "pool-1",
		mzColocationGroup: "1",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-2", "4000m", "4G", "7", map[string]string{
		ibColocationGroup: "pool-1",
		mzColocationGroup: "2",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-3", "4000m", "4G", "6", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "1",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-4", "4000m", "4G", "7", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "2",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-5", "4000m", "4G", "7", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "1",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-6", "4000m", "4G", "7", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "2",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-7", "4000m", "4G", "7", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "1",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-8", "4000m", "4G", "7", map[string]string{
		ibColocationGroup: "pool-2",
		mzColocationGroup: "2",
	}))
	mzNodes := make(map[string]string, 0)
	mzNodes["node-1"] = "1"
	mzNodes["node-2"] = "2"
	mzNodes["node-3"] = "1"
	mzNodes["node-4"] = "2"
	mzNodes["node-5"] = "1"
	mzNodes["node-6"] = "2"
	mzNodes["node-7"] = "1"
	mzNodes["node-8"] = "2"

	schedulerCache.BindFlowChannel = make(chan *cache.BindContext, 5000)
	framework.RegisterPluginBuilder(ColocatePluginName, NewColocatePlugin)
	framework.RegisterPluginBuilder("binpack", binpack.New)
	option := options.NewServerOption()
	option.PercentageOfNodesToFind = 100
	option.RegisterOptions()
	ssn := framework.OpenSession(schedulerCache, []conf.Tier{
		{
			Plugins: []conf.PluginOption{
				{
					Name:             ColocatePluginName,
					EnabledPredicate: &enabled,
					EnabledNodeOrder: &enabled,
					Arguments: map[string]interface{}{
						mzScoreArgumentKey: 20.0,
						mzPluginName:       true,
						ibPluginName:       true,
					},
				},
				{
					Name:             "binpack",
					EnabledNodeOrder: &enabled,
					Arguments:        map[string]interface{}{},
				},
			},
		},
	},
		[]conf.Configuration{
			{
				Name: "allocate",
			},
		},
	)
	allocate := allocate.New()
	allocate.Execute(ssn)
	klog.Infof("pg-1/pod-1 : %s; pg-1/pod-2 : %s; pg-1/pod-3 : %s", ssn.Jobs[api.JobID("ns-1/pg-1")].Tasks["pod-1"].NodeName, ssn.Jobs[api.JobID("ns-1/pg-1")].Tasks["pod-2"].NodeName, ssn.Jobs[api.JobID("ns-1/pg-1")].Tasks["pod-3"].NodeName)
	klog.Infof("pg-2/pod-1 : %s; pg-2/pod-2 : %s", ssn.Jobs[api.JobID("ns-2/pg-2")].Tasks["pod-1"].NodeName, ssn.Jobs[api.JobID("ns-2/pg-2")].Tasks["pod-2"].NodeName)

	assert.Assert(mzNodes[ssn.Jobs[api.JobID("ns-1/pg-1")].Tasks["pod-1"].NodeName] == "1", "Task should be scheduled on MZ 1")
	assert.Assert(mzNodes[ssn.Jobs[api.JobID("ns-1/pg-1")].Tasks["pod-2"].NodeName] == "1", "Task should be scheduled on MZ 1")
	assert.Assert(mzNodes[ssn.Jobs[api.JobID("ns-1/pg-1")].Tasks["pod-3"].NodeName] == "1", "Task should be scheduled on MZ 1")

	assert.Assert(mzNodes[ssn.Jobs[api.JobID("ns-2/pg-2")].Tasks["pod-1"].NodeName] == "1", "Task should be scheduled on MZ 1")
	assert.Assert(mzNodes[ssn.Jobs[api.JobID("ns-2/pg-2")].Tasks["pod-2"].NodeName] == "1", "Task should be scheduled on MZ 1")
}

func TestIBSorting(t *testing.T) {
	ngs := &NodeGroupList{
		items: []*NodeGroup{
			{
				name:        "pool-1",
				schedulable: 2,
				score:       80,
				resource: &nodeGroupResource{
					avialableCPU: 70.0,
					availableMem: 70.0,
					availableScalarResource: map[v1.ResourceName]float64{
						GPUResourceName: 70.0,
					},
				},
			},
			{
				name:        "pool-2",
				schedulable: 2,
				score:       80,
				resource: &nodeGroupResource{
					avialableCPU: 70.0,
					availableMem: 70.0,
					availableScalarResource: map[v1.ResourceName]float64{
						GPUResourceName: 75.0,
					},
				},
			},
			{
				name:        "pool-3",
				schedulable: 4,
				score:       80,
				resource: &nodeGroupResource{
					avialableCPU: 70.0,
					availableMem: 70.0,
					availableScalarResource: map[v1.ResourceName]float64{
						GPUResourceName: 70.0,
					},
				},
			},
			{
				name:        "pool-4",
				schedulable: 4,
				score:       90,
				resource: &nodeGroupResource{
					avialableCPU: 70.0,
					availableMem: 70.0,
					availableScalarResource: map[v1.ResourceName]float64{
						GPUResourceName: 70.0,
					},
				},
			},
		},
		lessFn: less,
	}
	sort.Sort(ngs)
	assert.Assert(ngs.items[0].name == "pool-4", "Expected pool-4")
	assert.Assert(ngs.items[1].name == "pool-3", "Expected pool-3")
	assert.Assert(ngs.items[2].name == "pool-1", "Expected pool-1")
	assert.Assert(ngs.items[3].name == "pool-2", "Expected pool-2")
}

func TestMZSorting(t *testing.T) {
	ngs := &NodeGroupList{
		items: []*NodeGroup{
			{
				name:        "pool-1",
				schedulable: 2,
				score:       80,
				resource: &nodeGroupResource{
					avialableCPU: 70.0,
					availableMem: 70.0,
					availableScalarResource: map[v1.ResourceName]float64{
						GPUResourceName: 70.0,
					},
				},
			},
			{
				name:        "pool-2",
				schedulable: 2,
				score:       80,
				resource: &nodeGroupResource{
					avialableCPU: 70.0,
					availableMem: 70.0,
					availableScalarResource: map[v1.ResourceName]float64{
						GPUResourceName: 75.0,
					},
				},
			},
			{
				name:        "pool-3",
				schedulable: 4,
				score:       80,
				resource: &nodeGroupResource{
					avialableCPU: 70.0,
					availableMem: 70.0,
					availableScalarResource: map[v1.ResourceName]float64{
						GPUResourceName: 70.0,
					},
				},
			},
			{
				name:        "pool-4",
				schedulable: 4,
				score:       90,
				resource: &nodeGroupResource{
					avialableCPU: 70.0,
					availableMem: 70.0,
					availableScalarResource: map[v1.ResourceName]float64{
						GPUResourceName: 70.0,
					},
				},
			},
		},
		lessFn: mzComparator,
	}
	sort.Sort(ngs)
	assert.Assert(ngs.items[0].name == "pool-4", "Expected pool-4")
	assert.Assert(ngs.items[1].name == "pool-3", "Expected pool-3")
	assert.Assert(ngs.items[2].name == "pool-1", "Expected pool-2")
	assert.Assert(ngs.items[3].name == "pool-2", "Expected pool-1")
}

func TestColocate_PoolMigration(t *testing.T) {
	enabled := true
	schedulerCache := cache.NewDefaultMockSchedulerCache("volcano")
	schedulerCache.Queues = map[api.QueueID]*api.QueueInfo{
		"default": {
			UID:    "default",
			Name:   "default",
			Weight: 1,
		},
	}
	schedulerCache.AddPodGroupV1beta1(&schedulingv1beta1.PodGroup{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PodGroup",
			APIVersion: "scheduling.volcano.sh/v1beta1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pg",
			UID:       "pg",
			Namespace: "ns-1",
		},
		Spec: schedulingv1beta1.PodGroupSpec{
			Queue: "default",
		},
	})
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-1", "", "pod-1", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg",
			collocateAnnotation:            "required",
		}))
	schedulerCache.AddPod(buildPod("ns-1", "mjuluri-pod-2", "", "pod-2", v1.PodPending, buildResourceList("2000m", "2G", "5"),
		[]metav1.OwnerReference{
			{
				Kind:       "PodGroup",
				Name:       "pg",
				APIVersion: "scheduling.volcano.sh/v1beta1",
				UID:        "pg",
			},
		}, map[string]string{}, map[string]string{
			"scheduling.k8s.io/group-name": "pg",
			collocateAnnotation:            "required",
		}))
	schedulerCache.AddOrUpdateNode(buildNode("node-1", "3000m", "4G", "5", map[string]string{
		"node.linkedin.com/pool": "pool-1",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-2", "4000m", "4G", "5", map[string]string{
		"node.linkedin.com/pool": "nimbus-training-nvidia-gpu-36feb",
	}))
	schedulerCache.AddOrUpdateNode(buildNode("node-3", "4000m", "4G", "5", map[string]string{
		"node.linkedin.com/pool": "nimbus-gpu-nvidia-h100-ssd-no-mig-kjp-2",
	}))
	schedulerCache.BindFlowChannel = make(chan *cache.BindContext, 5000)
	framework.RegisterPluginBuilder(ColocatePluginName, NewColocatePlugin)
	framework.RegisterPluginBuilder("binpack", binpack.New)
	option := options.NewServerOption()
	option.PercentageOfNodesToFind = 100
	option.RegisterOptions()
	ssn := framework.OpenSession(schedulerCache, []conf.Tier{
		{
			Plugins: []conf.PluginOption{
				{
					Name:             ColocatePluginName,
					EnabledPredicate: &enabled,
					EnabledNodeOrder: &enabled,
					Arguments: map[string]interface{}{
						mzScoreArgumentKey: 20.0,
						mzPluginName:       true,
						ibPluginName:       true,
					},
				},
				{
					Name:             "binpack",
					EnabledNodeOrder: &enabled,
					Arguments:        map[string]interface{}{},
				},
			},
		},
	},
		[]conf.Configuration{
			{
				Name: "allocate",
			},
		},
	)
	allocate := allocate.New()
	allocate.Execute(ssn)
	klog.Infof("Task1 : %s Task2 : %s", ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-1"].NodeName, ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-2"].NodeName)
	pod1Node := ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-1"].NodeName
	pod2Node := ssn.Jobs[api.JobID("ns-1/pg")].Tasks["pod-2"].NodeName
	assert.Assert(pod1Node != "node-1" && (pod1Node == "node-2" || pod1Node == "node-3"), "Task should not be scheduled on node-1")
	assert.Assert(pod2Node != "node-1" && (pod2Node == "node-2" || pod2Node == "node-3"), "Task should not be scheduled on node-1")
}

func buildPod(ns, n, nodeName, uid string, p v1.PodPhase, req v1.ResourceList, owner []metav1.OwnerReference,
	labels, annotations map[string]string) *v1.Pod {
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

func buildNode(name, cpu, mem, gpu string, labels map[string]string) *v1.Node {
	return util.BuildNode(name, api.BuildResourceListWithGPU(cpu, mem, gpu, []api.ScalarResource{{Name: "pods", Value: "2"}}...), labels)
}

func buildResourceList(cpu, memory, gpu string) v1.ResourceList {
	return api.BuildResourceListWithGPU(cpu, memory, gpu)
}
