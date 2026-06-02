package smg

import (
	"context"
	"log"
)

type EventType string
type EventCallback func(ctx context.Context, smgContext *ShardManagerContext, event Event) error

const (
	WorkerEvent  EventType = "worker"
	WorkerJoined EventType = "worker_joined"
	WorkerLeft   EventType = "worker_left"
)

type Event struct {
	EventType EventType
	Data      interface{}
}

type EventQueue struct {
	events     chan Event
	callback   EventCallback
	smgContext *ShardManagerContext
}

func (q *EventQueue) Enqueue(ctx context.Context, event Event) error {
	q.events <- event
	return nil
}

func (q *EventQueue) Start(ctx context.Context) error {
	go func() {
		for {
			select {
			case event := <-q.events:
				err := q.process(ctx, q.smgContext, event)
				if err != nil {
					log.Printf("failed to process event: %v", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

func (q *EventQueue) process(ctx context.Context, smgContext *ShardManagerContext, event Event) error {
	return q.callback(ctx, smgContext, event)
}

func NewEventQueue(callback EventCallback, smgContext *ShardManagerContext) *EventQueue {
	return &EventQueue{
		events:     make(chan Event, 1000),
		callback:   callback,
		smgContext: smgContext,
	}
}
