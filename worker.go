package binlog

import (
	"context"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

func NewWorker(db Database, startPosition mysql.Position) *worker {
	return &worker{
		database:   db,
		checkpoint: startPosition,
		nextJobID:  0,
	}
}

type job struct {
	handler   Handler
	cleanup   Cleanup
	predicate Predicate
}

type Handler func(ctx context.Context, event *Event) error

type Cleanup func()

type worker struct {
	database     Database
	checkpoint   mysql.Position
	syncer       *replication.BinlogSyncer
	streamer     *replication.BinlogStreamer
	nextPosition mysql.Position
	jobs         map[int]job
	nextJobID    int
}

func (w *worker) attach(job job) {
	w.jobs[w.nextJobID] = job
	w.nextJobID += 1
}

func (w *worker) work(ctx context.Context) error {
	err := w.connect()
	if err != nil {
		return err
	}
	event, err := w.getEvent(ctx)
	if err != nil {
		return err
	}
	w.handleEvent(ctx, event)
	return nil
}

func (w *worker) connect() error {
	if w.syncer == nil {
		w.syncer = w.database.newBinlogSyncer()
	}
	if w.streamer == nil {
		streamer, err := w.syncer.StartSync(w.checkpoint)
		if err != nil {
			return err
		}
		w.streamer = streamer
		w.nextPosition = w.checkpoint
	}
	return nil
}

func (w *worker) getEvent(ctx context.Context) (*Event, error) {
	binlogEvent, err := w.streamer.GetEvent(ctx)
	if err != nil {
		return nil, err
	}
	event := &Event{BinlogEvent: binlogEvent, Position: w.nextPosition}

	w.nextPosition.Pos = event.Header.LogPos
	rotateEvent, ok := event.Event.(*replication.RotateEvent)
	if ok {
		w.nextPosition.Name = string(rotateEvent.NextLogName)
	}

	return event, nil
}

func (w *worker) handleEvent(ctx context.Context, event *Event) {
	toDetach := []int{}
	for id, job := range w.jobs {
		err := job.handler(ctx, event)
		if err != nil || job.predicate(event) {
			toDetach = append(toDetach, id)
		}
	}
	for _, id := range toDetach {
		w.jobs[id].cleanup()
		delete(w.jobs, id)
	}
	w.checkpoint = event.Position
}
