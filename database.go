package binlog

import (
	"errors"
	"fmt"

	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

var (
	ErrEmptyMasterStatus = errors.New("'SHOW MASTER STATUS;' returned empty.")
)

type Database struct {
	Host     string
	Port     uint16
	User     string
	Password string
	DB       string
	ServerID uint32
}

func (db Database) newBinlogSyncer() *replication.BinlogSyncer {
	config := replication.BinlogSyncerConfig{
		ServerID: db.ServerID,
		Flavor:   "mysql",
		Host:     db.Host,
		Port:     db.Port,
		User:     db.User,
		Password: db.Password,
	}
	return replication.NewBinlogSyncer(&config)
}

func (db Database) latestBinlogPosition() (mysql.Position, error) {
	var pos mysql.Position

	conn, err := db.connect()
	if err != nil {
		return pos, err
	}

	result, err := conn.Execute("SHOW MASTER STATUS")
	if err != nil {
		return pos, err
	}
	if len(result.Values) == 0 {
		return pos, ErrEmptyMasterStatus
	}
	pos.Name = string(result.Values[0][0].([]uint8))
	pos.Pos = uint32(result.Values[0][1].(uint64))
	return pos, nil
}

func (db Database) connect() (*client.Conn, error) {
	return client.Connect(db.addr(), db.User, db.Password, db.DB)
}

func (db Database) addr() string {
	return fmt.Sprintf("%s:%d", db.Host, db.Port)
}
