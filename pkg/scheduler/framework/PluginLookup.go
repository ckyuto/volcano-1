package framework

import "volcano.sh/volcano/pkg/scheduler/api"

func GetReclaimableFn(ssn *Session, pluginName string) (api.EvictableFn, bool) {
	fn, found := ssn.reclaimableFns[pluginName]
	return fn, found
}

func GetOverUsedFn(ssn *Session, pluginName string) (api.ValidateFn, bool) {
	fn, found := ssn.overusedFns[pluginName]
	return fn, found
}

func GetAllocatableFn(ssn *Session, pluginName string) (api.AllocatableFn, bool) {
	fn, found := ssn.allocatableFns[pluginName]
	return fn, found
}

func GetJobEnqueueableFn(ssn *Session, pluginName string) (api.VoteFn, bool) {
	fn, found := ssn.jobEnqueueableFns[pluginName]
	return fn, found
}

func GetQueueOrderFn(ssn *Session, pluginName string) (api.CompareFn, bool) {
	fn, found := ssn.queueOrderFns[pluginName]
	return fn, found
}
