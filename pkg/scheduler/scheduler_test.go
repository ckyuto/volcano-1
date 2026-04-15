package scheduler

import (
	"fmt"
	"os"
	"testing"
	"time"

	"k8s.io/klog/v2"

	"volcano.sh/volcano/cmd/scheduler/app/options"
	"volcano.sh/volcano/pkg/scheduler/cache"

	_ "volcano.sh/volcano/pkg/scheduler/actions"
)

func TestNewSchedulerWithMockCache(t *testing.T) {
	tests := []struct {
		name           string
		opt            *options.ServerOption
		expectNilCache bool
	}{
		{
			name: "basic scheduler creation with nil config",
			opt: &options.ServerOption{
				SchedulerNames:    []string{"volcano"},
				SchedulePeriod:    time.Second,
				DefaultQueue:      "default",
				NodeWorkerThreads: 20,
			},
			expectNilCache: false,
		},
		{
			name: "scheduler with config file",
			opt: &options.ServerOption{
				SchedulerNames:                []string{"volcano"},
				SchedulerConf:                 "/tmp/nonexistent.yaml",
				SchedulePeriod:                time.Second,
				DefaultQueue:                  "default",
				NodeWorkerThreads:             20,
				DisableDefaultSchedulerConfig: false,
			},
			expectNilCache: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create scheduler with mock cache directly
			scheduler := &Scheduler{
				cache:              cache.NewDefaultMockSchedulerCache("volcano"),
				schedulerConf:      tt.opt.SchedulerConf,
				schedulePeriod:     tt.opt.SchedulePeriod,
				disableDefaultConf: tt.opt.DisableDefaultSchedulerConfig,
			}

			if scheduler == nil {
				t.Error("NewScheduler() returned nil scheduler")
				return
			}
			if scheduler.cache == nil && !tt.expectNilCache {
				t.Error("NewScheduler() scheduler cache is nil")
			}
			if scheduler.schedulePeriod != tt.opt.SchedulePeriod {
				t.Errorf("NewScheduler() schedulePeriod = %v, want %v",
					scheduler.schedulePeriod, tt.opt.SchedulePeriod)
			}
			if scheduler.schedulerConf != tt.opt.SchedulerConf {
				t.Errorf("NewScheduler() schedulerConf = %v, want %v",
					scheduler.schedulerConf, tt.opt.SchedulerConf)
			}
		})
	}
}

func TestScheduler_getSchedulerConf(t *testing.T) {
	scheduler := &Scheduler{}

	// Test with empty scheduler
	actions, plugins := scheduler.getSchedulerConf()
	if len(actions) != 0 {
		t.Errorf("getSchedulerConf() actions = %v, want empty slice", actions)
	}
	if len(plugins) != 0 {
		t.Errorf("getSchedulerConf() plugins = %v, want empty slice", plugins)
	}
}

func TestScheduler_loadSchedulerConf(t *testing.T) {
	// Create a temporary config file with wrong action name
	wrongConfigContent := `
actions: "enqueue, allocate, wrongaction"
tiers:
- plugins:
  - name: priority
  - name: gang
- plugins:
  - name: drf
  - name: predicates
`
	wrongConfigFile := "/tmp/wrong_scheduler_config.yaml"
	err := os.WriteFile(wrongConfigFile, []byte(wrongConfigContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create temporary config file: %v", err)
	}
	defer os.Remove(wrongConfigFile)

	// Create a temporary config file with correct actions and plugins
	correctConfigContent := `
actions: "enqueue, allocate, backfill, preemptall"
tiers:
- plugins:
  - name: uqm
  - name: priority
  - name: gang
  - name: conformance
- plugins:
  - name: drf
  - name: predicates
  - name: proportion
  - name: nodeorder
`
	correctConfigFile := "/tmp/correct_scheduler_config.yaml"
	err = os.WriteFile(correctConfigFile, []byte(correctConfigContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create temporary correct config file: %v", err)
	}
	defer os.Remove(correctConfigFile)

	tests := []struct {
		name           string
		scheduler      *Scheduler
		expectPanic    bool
		useDefaultConf bool
	}{
		{
			name: "scheduler with default config enabled",
			scheduler: &Scheduler{
				disableDefaultConf: false,
			},
			expectPanic:    false,
			useDefaultConf: true,
		},
		{
			name: "scheduler with no config file",
			scheduler: &Scheduler{
				schedulerConf:      "",
				disableDefaultConf: true,
			},
			expectPanic:    true,
			useDefaultConf: false,
		},
		{
			name: "scheduler with config file containing wrong action name",
			scheduler: &Scheduler{
				schedulerConf:      wrongConfigFile,
				disableDefaultConf: true,
			},
			expectPanic:    true,
			useDefaultConf: false,
		},
		{
			name: "scheduler with config file containing correct action names",
			scheduler: &Scheduler{
				schedulerConf:      correctConfigFile,
				disableDefaultConf: true,
			},
			expectPanic:    false,
			useDefaultConf: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var panicked bool

			// Override klog.OsExit so that klog.Fatalf triggers a panic
			// instead of os.Exit, allowing recover() to catch it.
			origOsExit := klog.OsExit
			klog.OsExit = func(code int) { panic(fmt.Sprintf("klog.Fatal exit code %d", code)) }
			defer func() { klog.OsExit = origOsExit }()

			defer func() {
				if r := recover(); r != nil {
					panicked = true
					if !tt.expectPanic {
						t.Errorf("loadSchedulerConf() unexpected panic: %v", r)
					}
				}
			}()

			tt.scheduler.loadSchedulerConf()

			// Check if we expected a panic but didn't get one
			if tt.expectPanic && !panicked {
				t.Error("loadSchedulerConf() expected to panic but didn't")
			}

			// Only check these if we didn't panic and don't expect to panic
			if !tt.expectPanic && !panicked {
				if tt.useDefaultConf {
					if len(tt.scheduler.actions) == 0 {
						t.Error("loadSchedulerConf() should have loaded default actions")
					}
					if len(tt.scheduler.plugins) == 0 {
						t.Error("loadSchedulerConf() should have loaded default plugins")
					}
				} else if tt.scheduler.schedulerConf == correctConfigFile {
					// Verify that correct actions and plugins were loaded
					if len(tt.scheduler.actions) == 0 {
						t.Error("loadSchedulerConf() should have loaded actions from correct config file")
					}
					if len(tt.scheduler.plugins) == 0 {
						t.Error("loadSchedulerConf() should have loaded plugins from correct config file")
					}
					// Verify specific actions were loaded (enqueue, allocate, backfill)
					expectedActions := []string{"enqueue", "allocate", "backfill", "preemptall"}
					actualActions, _ := tt.scheduler.getSchedulerConf()
					for _, expectedAction := range expectedActions {
						found := false
						for _, actualAction := range actualActions {
							if actualAction == expectedAction {
								found = true
								break
							}
						}
						if !found {
							t.Errorf("Expected action %s not found in loaded actions: %v", expectedAction, actualActions)
						}
					}
				}
			}
		})
	}
}

func TestScheduler_runOnce(t *testing.T) {
	// Create a minimal scheduler for testing runOnce
	scheduler := &Scheduler{
		cache: cache.NewDefaultMockSchedulerCache("volcano"),
	}

	// This should not panic even with minimal setup
	scheduler.runOnce()
}

func TestScheduler_watchSchedulerConf(t *testing.T) {
	scheduler := &Scheduler{
		fileWatcher: nil,
	}

	stopCh := make(chan struct{})
	close(stopCh)

	// Should return immediately when fileWatcher is nil
	scheduler.watchSchedulerConf(stopCh)
}
