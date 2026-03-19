package main

import (
	"context"
	"fmt"
	"sync"
)

// ConversionJob represents a conversion task
type ConversionJob struct {
	HTML   string
	Result chan ConversionResult
}

// ConversionResult represents the result of a conversion
type ConversionResult struct {
	Markdown string
	Error    error
}

// WorkerPool manages concurrent conversion workers
type WorkerPool struct {
	workers   int
	jobs      chan ConversionJob
	converter *Converter
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(workers int, converter *Converter) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())
	
	pool := &WorkerPool{
		workers:   workers,
		jobs:      make(chan ConversionJob, workers*2), // Buffer for smoother operation
		converter: converter,
		ctx:       ctx,
		cancel:    cancel,
	}
	
	pool.start()
	return pool
}

// start initializes and starts all workers
func (p *WorkerPool) start() {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
}

// worker processes conversion jobs
func (p *WorkerPool) worker(id int) {
	defer p.wg.Done()
	
	for {
		select {
		case <-p.ctx.Done():
			return
		case job, ok := <-p.jobs:
			if !ok {
				return
			}
			
			markdown, err := p.converter.ConvertHTMLToMarkdown(job.HTML)
			job.Result <- ConversionResult{
				Markdown: markdown,
				Error:    err,
			}
			close(job.Result)
		}
	}
}

// Submit submits a conversion job and waits for result
func (p *WorkerPool) Submit(html string) (string, error) {
	resultChan := make(chan ConversionResult, 1)
	
	job := ConversionJob{
		HTML:   html,
		Result: resultChan,
	}
	
	select {
	case p.jobs <- job:
		result := <-resultChan
		return result.Markdown, result.Error
	case <-p.ctx.Done():
		return "", fmt.Errorf("worker pool is shutting down")
	}
}

// Shutdown gracefully shuts down the worker pool
func (p *WorkerPool) Shutdown() {
	p.cancel()
	close(p.jobs)
	p.wg.Wait()
}
