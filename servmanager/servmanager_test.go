/*
Real-time Online/Offline Charging System (OCS) for Telecom & ISP environments
Copyright (C) ITsysCOM GmbH

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>
*/

package servmanager

import (
	"sync"
	"testing"
	"time"

	"github.com/cgrates/cgrates/config"
	"github.com/cgrates/cgrates/utils"
)

type mockService struct {
	name      string
	start     func(*mockService) error
	reload    func(*mockService) error
	shutdown  func(*mockService) error
	isRunning bool
	shouldRun bool
}

func newMockService(name string) *mockService {
	return &mockService{
		name:      name,
		start:     func(*mockService) error { return nil },
		reload:    func(*mockService) error { return nil },
		shutdown:  func(*mockService) error { return nil },
		shouldRun: true,
	}
}

func (m *mockService) Start() error {
	return m.start(m)
}
func (m *mockService) Reload() error {
	return m.reload(m)
}
func (m *mockService) Shutdown() error {
	return m.shutdown(m)
}
func (m *mockService) IsRunning() bool {
	return m.isRunning
}
func (m *mockService) ShouldRun() bool {
	return m.shouldRun
}
func (m *mockService) ServiceName() string {
	return m.name
}

func waitWithTimeout(t *testing.T, ch <-chan struct{}, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatal(msg)
	}
}

func TestV1StartStopServiceScheduler(t *testing.T) {
	cfg := config.NewDefaultCGRConfig()
	sm := NewServiceManager(cfg, utils.NewSyncedChan(), new(sync.WaitGroup), nil)
	var reply string

	startReloadDone := make(chan struct{})
	go func() {
		<-cfg.GetReloadChan(config.SCHEDULER_JSN)
		close(startReloadDone)
	}()
	if err := sm.V1StartService(nil, ArgStartService{ServiceID: utils.MetaScheduler}, &reply); err != nil {
		t.Fatalf("unexpected error on start: %v", err)
	}
	if reply != utils.OK {
		t.Fatalf("unexpected start reply: %s", reply)
	}
	if !cfg.SchedulerCfg().Enabled {
		t.Fatal("scheduler must be enabled after start")
	}
	waitWithTimeout(t, startReloadDone, time.Second, "start did not trigger reload")

	stopReloadDone := make(chan struct{})
	go func() {
		<-cfg.GetReloadChan(config.SCHEDULER_JSN)
		close(stopReloadDone)
	}()
	if err := sm.V1StopService(nil, ArgStartService{ServiceID: utils.MetaScheduler}, &reply); err != nil {
		t.Fatalf("unexpected error on stop: %v", err)
	}
	if reply != utils.OK {
		t.Fatalf("unexpected stop reply: %s", reply)
	}
	if cfg.SchedulerCfg().Enabled {
		t.Fatal("scheduler must be disabled after stop")
	}
	waitWithTimeout(t, stopReloadDone, time.Second, "stop did not trigger reload")
}

func TestV1ServiceMethodsUnsupportedID(t *testing.T) {
	cfg := config.NewDefaultCGRConfig()
	sm := NewServiceManager(cfg, utils.NewSyncedChan(), new(sync.WaitGroup), nil)
	var reply string
	args := ArgStartService{ServiceID: "unknown-service"}

	if err := sm.V1StartService(nil, args, &reply); err == nil {
		t.Fatal("expected start error for unsupported service")
	}
	if err := sm.V1StopService(nil, args, &reply); err == nil {
		t.Fatal("expected stop error for unsupported service")
	}
	if err := sm.V1ServiceStatus(nil, args, &reply); err == nil {
		t.Fatal("expected status error for unsupported service")
	}
}

func TestV1ServiceStatus(t *testing.T) {
	cfg := config.NewDefaultCGRConfig()
	sm := NewServiceManager(cfg, utils.NewSyncedChan(), new(sync.WaitGroup), nil)
	srv := newMockService(utils.SchedulerS)
	sm.AddServices(srv)
	var reply string

	srv.isRunning = true
	if err := sm.V1ServiceStatus(nil, ArgStartService{ServiceID: utils.MetaScheduler}, &reply); err != nil {
		t.Fatalf("unexpected status error: %v", err)
	}
	if reply != utils.RunningCaps {
		t.Fatalf("expected running status, got %q", reply)
	}

	srv.isRunning = false
	if err := sm.V1ServiceStatus(nil, ArgStartService{ServiceID: utils.MetaScheduler}, &reply); err != nil {
		t.Fatalf("unexpected status error: %v", err)
	}
	if reply != utils.StoppedCaps {
		t.Fatalf("expected stopped status, got %q", reply)
	}
}

func TestAddServicesDoesNotOverwrite(t *testing.T) {
	cfg := config.NewDefaultCGRConfig()
	sm := NewServiceManager(cfg, utils.NewSyncedChan(), new(sync.WaitGroup), nil)
	first := newMockService(utils.SchedulerS)
	second := newMockService(utils.SchedulerS)

	sm.AddServices(first)
	sm.AddServices(second)

	if got := sm.GetService(utils.SchedulerS); got != first {
		t.Fatal("service with same name should not be overwritten")
	}
}

func TestReloadServiceLifecycle(t *testing.T) {
	t.Run("reload_running_service", func(t *testing.T) {
		cfg := config.NewDefaultCGRConfig()
		sm := NewServiceManager(cfg, utils.NewSyncedChan(), new(sync.WaitGroup), nil)
		srv := newMockService(utils.SchedulerS)
		srv.isRunning = true
		reloaded := false
		srv.reload = func(*mockService) error {
			reloaded = true
			return nil
		}
		sm.AddServices(srv)
		if err := sm.reloadService(utils.SchedulerS); err != nil {
			t.Fatalf("unexpected reload error: %v", err)
		}
		if !reloaded {
			t.Fatal("expected reload to be called")
		}
	})

	t.Run("start_stopped_service", func(t *testing.T) {
		cfg := config.NewDefaultCGRConfig()
		sm := NewServiceManager(cfg, utils.NewSyncedChan(), new(sync.WaitGroup), nil)
		srv := newMockService(utils.SchedulerS)
		started := false
		srv.start = func(m *mockService) error {
			started = true
			m.isRunning = true
			return nil
		}
		sm.AddServices(srv)
		if err := sm.reloadService(utils.SchedulerS); err != nil {
			t.Fatalf("unexpected start error: %v", err)
		}
		if !started || !srv.isRunning {
			t.Fatal("expected start to run and set service running")
		}
	})

	t.Run("shutdown_running_service", func(t *testing.T) {
		cfg := config.NewDefaultCGRConfig()
		shdWg := new(sync.WaitGroup)
		sm := NewServiceManager(cfg, utils.NewSyncedChan(), shdWg, nil)
		srv := newMockService(utils.SchedulerS)
		srv.isRunning = true
		srv.shouldRun = false
		shdWg.Add(1)
		srv.shutdown = func(m *mockService) error {
			m.isRunning = false
			return nil
		}
		sm.AddServices(srv)
		if err := sm.reloadService(utils.SchedulerS); err != nil {
			t.Fatalf("unexpected shutdown error: %v", err)
		}

		waitDone := make(chan struct{})
		go func() {
			shdWg.Wait()
			close(waitDone)
		}()
		waitWithTimeout(t, waitDone, time.Second, "waitgroup was not decremented on shutdown")
		if srv.isRunning {
			t.Fatal("service should be stopped")
		}
	})
}

func TestReloadServiceErrorClosesShutdownChannel(t *testing.T) {
	t.Run("reload_error", func(t *testing.T) {
		cfg := config.NewDefaultCGRConfig()
		shdChan := utils.NewSyncedChan()
		sm := NewServiceManager(cfg, shdChan, new(sync.WaitGroup), nil)
		srv := newMockService(utils.SchedulerS)
		srv.isRunning = true
		srv.reload = func(*mockService) error { return utils.ErrNotFound }
		sm.AddServices(srv)
		if err := sm.reloadService(utils.SchedulerS); err == nil {
			t.Fatal("expected reload error")
		}
		select {
		case <-shdChan.Done():
		default:
			t.Fatal("expected shutdown channel to be closed on reload error")
		}
	})

	t.Run("start_error", func(t *testing.T) {
		cfg := config.NewDefaultCGRConfig()
		shdChan := utils.NewSyncedChan()
		sm := NewServiceManager(cfg, shdChan, new(sync.WaitGroup), nil)
		srv := newMockService(utils.SchedulerS)
		srv.start = func(*mockService) error { return utils.ErrNotFound }
		sm.AddServices(srv)
		if err := sm.reloadService(utils.SchedulerS); err == nil {
			t.Fatal("expected start error")
		}
		select {
		case <-shdChan.Done():
		default:
			t.Fatal("expected shutdown channel to be closed on start error")
		}
	})

	t.Run("shutdown_error", func(t *testing.T) {
		cfg := config.NewDefaultCGRConfig()
		shdChan := utils.NewSyncedChan()
		shdWg := new(sync.WaitGroup)
		sm := NewServiceManager(cfg, shdChan, shdWg, nil)
		srv := newMockService(utils.SchedulerS)
		srv.isRunning = true
		srv.shouldRun = false
		srv.shutdown = func(*mockService) error { return utils.ErrNotFound }
		shdWg.Add(1)
		sm.AddServices(srv)
		if err := sm.reloadService(utils.SchedulerS); err == nil {
			t.Fatal("expected shutdown error")
		}
		select {
		case <-shdChan.Done():
		default:
			t.Fatal("expected shutdown channel to be closed on shutdown error")
		}
	})
}

func TestStartAndShutdownServices(t *testing.T) {
	cfg := config.NewDefaultCGRConfig()
	shdChan := utils.NewSyncedChan()
	shdWg := new(sync.WaitGroup)
	sm := NewServiceManager(cfg, shdChan, shdWg, nil)
	srv := newMockService(utils.SchedulerS)
	started := make(chan struct{})
	srv.start = func(m *mockService) error {
		m.isRunning = true
		close(started)
		return nil
	}
	srv.shutdown = func(m *mockService) error {
		m.isRunning = false
		return nil
	}
	sm.AddServices(srv)

	if err := sm.StartServices(); err != nil {
		t.Fatalf("unexpected start services error: %v", err)
	}
	waitWithTimeout(t, started, time.Second, "service start was not triggered")

	shdChan.CloseOnce()
	waitDone := make(chan struct{})
	go func() {
		shdWg.Wait()
		close(waitDone)
	}()
	waitWithTimeout(t, waitDone, time.Second, "services were not shutdown after close signal")
	if srv.isRunning {
		t.Fatal("service should be stopped after shutdown")
	}
}
