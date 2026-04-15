package proportion

import (
	"k8s.io/klog/v2"
	"strings"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/util"
)

// PluginName indicates name of volcano scheduler plugin.
const (
	UqmProportionPluginName = "uqmproportion"
	UqmDefaultQueueKey      = "uqm.defaultQueues"
	DefaultUqmQueue         = "uqm.default"
)

type uqmProportionPlugin struct {
	uqmDefaultQueues []string
	proportionPlugin *proportionPlugin
}

/*
This plugin represents an enhanced iteration of Volcano's proportion plugin, featuring a customized implementation.
1. Bypasses checks and validations for jobs submitted to UQM default queue.
2. Behaves as the standard proportion plugin otherwise.
*/

// return uqm version of proportion plugin
func NewUqmProportionPlugin(arguments framework.Arguments) framework.Plugin {
	var uqmDefaultQueues []string
	if queueNameArry, ok := arguments[UqmDefaultQueueKey].(string); ok {
		klog.Infof("configured uqm defaults queues %s", arguments[UqmDefaultQueueKey].(string))
		uqmDefaultQueues = strings.Split(queueNameArry, ",")
	} else {
		klog.Infof("no uqm default queues are configured, using %s", DefaultUqmQueue)
		uqmDefaultQueues = []string{DefaultUqmQueue}
	}
	p := &uqmProportionPlugin{
		uqmDefaultQueues: uqmDefaultQueues,
		proportionPlugin: &proportionPlugin{
			totalResource:   api.EmptyResource(),
			totalGuarantee:  api.EmptyResource(),
			queueOpts:       map[api.QueueID]*queueAttr{},
			pluginArguments: arguments,
		},
	}
	return p
}

func (pp *uqmProportionPlugin) Name() string {
	return UqmProportionPluginName
}

func (pp *uqmProportionPlugin) OnSessionOpen(ssn *framework.Session) {

	pp.proportionPlugin.OnSessionOpen(ssn)
	ssn.AddQueueOrderFn(pp.Name(), func(l, r interface{}) int {
		if fn, found := framework.GetQueueOrderFn(ssn, pp.proportionPlugin.Name()); found {
			return fn(l, r)
		}
		return 1
	})

	ssn.AddReclaimableFn(pp.Name(), func(reclaimer *api.TaskInfo, reclaimees []*api.TaskInfo) ([]*api.TaskInfo, int) {
		var victims []*api.TaskInfo
		//If the tasks' queue is a uqm default queue, then skip validations
		if jobInfo, found := ssn.Jobs[reclaimer.Job]; found {
			if queue, ok := ssn.Queues[jobInfo.Queue]; ok {
				if contains(pp.uqmDefaultQueues, queue.Name) {
					klog.Infof("ignoring AddReclaimableFn for %s", reclaimer.Name)
					return victims, util.Permit
				}
			}
		}

		klog.V(5).Infof("executing AddReclaimableFn for %s", reclaimer.Name)
		if reclaimable, found := framework.GetReclaimableFn(ssn, pp.proportionPlugin.Name()); found {
			return reclaimable(reclaimer, reclaimees)
		}
		return victims, util.Permit
	})

	ssn.AddOverusedFn(pp.Name(), func(obj interface{}) bool {
		queue := obj.(*api.QueueInfo)
		//If the queue is a uqm default queue, then skip for queue level validations
		if contains(pp.uqmDefaultQueues, queue.Name) {
			klog.Infof("ignoring AddOverusedFn for %s", queue.Name)
			return false
		}

		klog.V(5).Infof("executing AddOverusedFn for %s", queue.Name)
		if fn, found := framework.GetOverUsedFn(ssn, pp.proportionPlugin.Name()); found {
			return fn(obj)
		}
		return false
	})

	ssn.AddAllocatableFn(pp.Name(), func(queue *api.QueueInfo, candidate *api.TaskInfo) bool {
		//If the queue is a uqm default queue, then skip for queue level validations
		if contains(pp.uqmDefaultQueues, queue.Name) {
			klog.Infof("ignoring AddAllocatableFn for task %s with queue %s", candidate.Name, queue.Name)
			return true
		}

		klog.V(5).Infof("executing AddAllocatableFn for task %s with queue %s", candidate.Name, queue.Name)
		if fn, found := framework.GetAllocatableFn(ssn, pp.proportionPlugin.Name()); found {
			return fn(queue, candidate)
		}
		return true
	})

	ssn.AddJobEnqueueableFn(pp.Name(), func(obj interface{}) int {
		job := obj.(*api.JobInfo)
		queueID := job.Queue
		queue := ssn.Queues[queueID]

		//If the queue is a uqm default queue, then skip for queue level validations
		if contains(pp.uqmDefaultQueues, queue.Name) {
			klog.Infof("ignoring AddJobEnqueueableFn for job %s with queue %s", job.Name, queue.Name)
			return util.Permit
		}
		klog.V(5).Infof("executing ddJobEnqueueableFn for job %s with queue %s", job.Name, queue.Name)
		if fn, found := framework.GetJobEnqueueableFn(ssn, pp.proportionPlugin.Name()); found {
			return fn(obj)
		}
		return util.Permit
	})
}

func (pp *uqmProportionPlugin) OnSessionClose(ssn *framework.Session) {
	pp.proportionPlugin.OnSessionClose(ssn)
}

func contains(queuesArry []string, item string) bool {
	if nil == queuesArry {
		return false
	}
	for _, value := range queuesArry {
		if value == item {
			return true
		}
	}
	return false
}
