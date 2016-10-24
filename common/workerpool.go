package common

import (
	bw "github.com/immesys/bw2bind"
)

func worker(in chan *bw.SimpleMessage, cb func(msg *bw.SimpleMessage)) {
	for msg := range in {
		cb(msg)
	}
}

type workerPool struct {
	in   chan *bw.SimpleMessage
	cb   func(msg *bw.SimpleMessage)
	size int
}

func NewWorkerPool(in chan *bw.SimpleMessage, cb func(msg *bw.SimpleMessage), size int) *workerPool {
	pool := &workerPool{
		in:   in,
		cb:   cb,
		size: size,
	}
	return pool
}

func (pool *workerPool) Start() {
	go func() {
		for msg := range pool.in {
			go pool.cb(msg)
		}
	}()
}
