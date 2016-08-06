package common

import (
	bw "gopkg.in/immesys/bw2bind.v5"
)

type worker struct {
	cb func(msg *bw.SimpleMessage)
}

func newWorker(cb func(msg *bw.SimpleMessage)) *worker {
	w := &worker{
		cb: cb,
	}
	return w
}

func (w *worker) run(msg *bw.SimpleMessage) {
	w.cb(msg)
}

type workerPool struct {
	workers chan *worker
	in      chan *bw.SimpleMessage
	cb      func(msg *bw.SimpleMessage)
}

func NewWorkerPool(in chan *bw.SimpleMessage, cb func(msg *bw.SimpleMessage), size int) *workerPool {
	pool := &workerPool{
		workers: make(chan *worker, size),
		in:      in,
		cb:      cb,
	}
	for i := 0; i < size; i++ {
		pool.workers <- newWorker(pool.cb)
	}

	return pool
}

func (pool *workerPool) Start() {
	go func() {
		for msg := range pool.in {
			select {
			case worker := <-pool.workers:
				go pool.handle(worker, msg)
			default:
				worker := newWorker(pool.cb)
				go pool.handle(worker, msg)
			}
		}
	}()
}

func (pool *workerPool) handle(dude *worker, msg *bw.SimpleMessage) {
	dude.run(msg)
	select {
	case pool.workers <- dude:
	default:
		dude = nil
	}
}
