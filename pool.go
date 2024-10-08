package pool

import (
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// errors
var (
	// ErrInvalidPoolCap return if pool size <= 0
	ErrInvalidPoolCap = errors.New("invalid pool cap")
	// ErrPoolAlreadyClosed put task but pool already closed
	ErrPoolAlreadyClosed = errors.New("pool already closed")
)

// running status
const (
	RUNNING = 1
	STOPED  = 0
)

// Task task to-do
type Task[T any] struct {
	Handler func(v T)
	Params  T
}

// Pool task pool
type Pool[T any] struct {
	capacity       uint64
	runningWorkers uint64
	status         int64
	chTask         chan *Task[T]
	PanicHandler   func(any)
	sync.Mutex
}

// NewPool init pool
func NewPool[T any](capacity uint64) (*Pool[T], error) {
	if capacity <= 0 {
		return nil, ErrInvalidPoolCap
	}
	p := &Pool[T]{
		capacity: capacity,
		status:   RUNNING,
		chTask:   make(chan *Task[T], capacity),
	}

	return p, nil
}

func (p *Pool[T]) checkWorker() {
	p.Lock()
	defer p.Unlock()

	if p.runningWorkers == 0 && len(p.chTask) > 0 {
		p.run()
	}
}

// GetCap get capacity
func (p *Pool[T]) GetCap() uint64 {
	return p.capacity
}

// GetRunningWorkers get running workers
func (p *Pool[T]) GetRunningWorkers() uint64 {
	return atomic.LoadUint64(&p.runningWorkers)
}

func (p *Pool[T]) incRunning() {
	atomic.AddUint64(&p.runningWorkers, 1)
}

func (p *Pool[T]) decRunning() {
	atomic.AddUint64(&p.runningWorkers, ^uint64(0))
}

// Put a task to pool
func (p *Pool[T]) Put(task *Task[T]) error {
	p.Lock()
	defer p.Unlock()

	if p.status == STOPED {
		return ErrPoolAlreadyClosed
	}

	// run worker
	if p.GetRunningWorkers() < p.GetCap() {
		p.run()
	}

	// send task
	if p.status == RUNNING {
		p.chTask <- task
	}

	return nil
}

func (p *Pool[T]) run() {
	p.incRunning()

	go func() {
		defer func() {
			p.decRunning()
			if r := recover(); r != nil {
				if p.PanicHandler != nil {
					p.PanicHandler(r)
				} else {
					log.Printf("Worker panic: %s\n", r)
				}
			}
			p.checkWorker() // check worker avoid no worker running
		}()

		for {
			select {
			case task, ok := <-p.chTask:
				if !ok {
					return
				}
				task.Handler(task.Params)
			}
		}
	}()
}

func (p *Pool[T]) setStatus(status int64) bool {
	p.Lock()
	defer p.Unlock()

	if p.status == status {
		return false
	}

	p.status = status

	return true
}

// Close pool graceful
func (p *Pool[T]) Close() {

	if !p.setStatus(STOPED) { // stop put task
		return
	}

	for len(p.chTask) > 0 { // wait all task be consumed
		time.Sleep(1e6) // reduce CPU load
	}

	close(p.chTask)
}
