package foundationfx

import (
	"github.com/Penthious/go-foundation/logger"
	"github.com/Penthious/go-foundation/scheduler"

	"go.uber.org/fx"
)

// SchedulerModule starts jobs in the foundation_jobs Fx value group and joins
// them at shutdown. Supply scheduler.Config; set DB through a config constructor
// when coordinated jobs are registered. The scheduler borrows its dependencies.
var SchedulerModule = fx.Module("go-foundation/scheduler",
	fx.Provide(newScheduler),
	fx.Invoke(func(*scheduler.Scheduler) {}),
)

type schedulerParams struct {
	fx.In

	Lifecycle fx.Lifecycle
	Logger    *logger.Logger
	Config    scheduler.Config
	Jobs      []scheduler.Job `group:"foundation_jobs"`
}

func newScheduler(p schedulerParams) (*scheduler.Scheduler, error) {
	cfg := p.Config
	if cfg.Logger == nil {
		cfg.Logger = p.Logger
	}

	worker, err := scheduler.New(cfg, p.Jobs...)
	if err != nil {
		return nil, err
	}

	p.Lifecycle.Append(fx.Hook{OnStart: worker.Start, OnStop: worker.Shutdown})

	return worker, nil
}
