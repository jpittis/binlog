package binlog

import (
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

type Range struct {
	StartPosition StartPosition
	EndPredicate  Predicate
}

type StartPosition func(Database) (mysql.Position, error)

type Predicate func(*Event) bool

type Event struct {
	*replication.BinlogEvent
	Position mysql.Position
}

func StartAt(position mysql.Position) StartPosition {
	return func(db Database) (mysql.Position, error) {
		return position, nil
	}
}

func Latest(db Database) (mysql.Position, error) {
	return db.latestBinlogPosition()
}

func EndAfter(end mysql.Position) Predicate {
	return func(event *Event) bool {
		current := event.Position
		return end.Name == current.Name && current.Pos >= end.Pos
	}
}

func NeverEnd(event *Event) bool {
	return false
}
