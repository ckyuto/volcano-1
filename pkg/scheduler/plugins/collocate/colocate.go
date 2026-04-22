package collocate

import (
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

const (
	ColocatePluginName  = "colocation"
	collocateAnnotation = "job.linkedin.com/collocate"
	GPUResourceName     = "nvidia.com/gpu"
	ibPluginName        = "ib"
	mzPluginName        = "mz"
)

// IBNodePoolMigrations maps each old IB NodePool to the new NodePool its nodes are being migrated to.
// Add an entry when a NodePool's configuration is changing and its nodes are being moved to a replacement pool.
var IBNodePoolMigrations = map[string]string{
	"nimbus-gpu-nvidia-h100-ssd-no-mig-kjp":   "gpu-nvidia-h100-ssd-volcano-hami-kjp",
	"nimbus-gpu-nvidia-h100-ssd-no-mig-kjp-2": "gpu-nvidia-h100-ssd-volcano-hami-kjp-2",
	"nimbus-gpu-nvidia-h200-no-mig-kjp":       "gpu-nvidia-h200-volcano-hami-kjp",
}

/**
* All the plugins that want to influence the scoring of colocation should implement this interface.
 */
type Colocation interface {
	/**
	* Given a list of nodes the pod can be scheduled on,
	* this function scores the nodes based on the colocation requirements.
	**/
	BatchNodeOrder(candidate *api.TaskInfo, nodes []*api.NodeInfo, ssn *framework.Session, ranker *NodeGroupRanker) (map[string]float64, error)
	/**
	* Given a candidate node, this function checks if the node satisfies the colocation requirements.
	**/
	Predicate(candidate *api.TaskInfo, node *api.NodeInfo) error
	/**
	* Cleanup any internal state maintained by the plugin.
	**/
	CloseSession(ssn *framework.Session)
	/**
	* Plugins can update their internal state when a task is allocated to a node.
	**/
	TaskAllocated(event *framework.Event, ssn *framework.Session)
	/**
	* Plugins can update their internal state when a task is deallocated from a node.
	**/
	TaskDeallocated(event *framework.Event, ssn *framework.Session)
}

type colocatePlugin struct {
	args        framework.Arguments
	colocations []Colocation
	ranker      *NodeGroupRanker
}

func NewColocatePlugin(arguments framework.Arguments) framework.Plugin {
	return &colocatePlugin{
		args:        arguments,
		colocations: make([]Colocation, 0),
		ranker:      newNodeGroupRanker(),
	}
}

func (p *colocatePlugin) Name() string {
	return ColocatePluginName
}

func (p *colocatePlugin) OnSessionOpen(ssn *framework.Session) {
	klog.V(3).Infof("Colocate plugin is started for this session with arguments %v", p.args)
	if p.args == nil {
		klog.Error("Colocate plugin arguments are nil")
		return
	}
	ibEnabled := false
	mzEnabled := false
	p.args.GetBool(&ibEnabled, "ib")
	p.args.GetBool(&mzEnabled, "mz")
	p.colocations = make([]Colocation, 0)
	if ibEnabled {
		ibColocation := newIBColocation(ssn)
		if ibColocation == nil {
			klog.Error("Failed to create the IB Colocation plugin")
		} else {
			p.colocations = append(p.colocations, ibColocation)
		}
	}
	if mzEnabled {
		mzColocation := newMZColocation(ssn, p.args)
		p.colocations = append(p.colocations, mzColocation)
	}

	ssn.AddPredicateFn(p.Name(), func(candidate *api.TaskInfo, node *api.NodeInfo) error {
		for _, colocate := range p.colocations {
			err := colocate.Predicate(candidate, node)
			if err != nil {
				return err
			}
		}
		return nil
	})

	ssn.AddBatchNodeOrderFn(p.Name(), func(candidate *api.TaskInfo, nodes []*api.NodeInfo) (map[string]float64, error) {
		nodeScores := make(map[string]float64)
		if !p.ranker.fakeScoring {
			for _, colocate := range p.colocations {
				scores, _ := colocate.BatchNodeOrder(candidate, nodes, ssn, p.ranker)
				for name, score := range scores {
					nodeScores[name] = nodeScores[name] + score
				}
			}
			klog.V(3).Infof("Job %s, Scores %v", candidate.Job, nodeScores)
		}
		return nodeScores, nil
	})

	ssn.AddEventHandler(&framework.EventHandler{
		AllocateFunc: func(event *framework.Event) {
			for _, colocate := range p.colocations {
				colocate.TaskAllocated(event, ssn)
			}
		},
		DeallocateFunc: func(event *framework.Event) {
			// We do not know if this can happen!
			// Lets say that this happens and we still retain the nodepool for the job.
			// It means that for the other Pods in this PodGroup we would end up rejecting other NodePools where IB is present.
			// And if there are no free resources in this NodePool, the other pods won't be scheduled.
			// But all this would be corrected in the next scheduling session. So this is a temporary issue and does not cause much harm.
			// Based on how frequently this happens, we can decide if we need to handle this case.
			klog.V(3).Infof("Deallocating Node %s for the Task %s from this Job %s", event.Task.NodeName, event.Task.Name, event.Task.Job)
			for _, colocate := range p.colocations {
				colocate.TaskDeallocated(event, ssn)
			}
		},
	})
}

func (p *colocatePlugin) OnSessionClose(ssn *framework.Session) {
	for _, colocate := range p.colocations {
		colocate.CloseSession(ssn)
	}
	p.colocations = nil
	p.ranker.fakeScoring = false
	p.ranker = nil
}
