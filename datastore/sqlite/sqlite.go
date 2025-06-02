package sqlite

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/teslamotors/fleet-telemetry/datastore"
	"github.com/teslamotors/fleet-telemetry/messages"
	"github.com/teslamotors/fleet-telemetry/server/airbrake"
)

// SQLiteStore is a sqlite backed datastore
type SQLiteStore struct {
	db *sql.DB
	tx *sql.Tx
}

// New returns a new sqlite store
func New(dbPath string) (datastore.Datastore, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	// Create table if not exists
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS records (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			txid TEXT NOT NULL,
			vin TEXT NOT NULL,
			type TEXT NOT NULL,
			json_data TEXT NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			topic TEXT,
			partition INTEGER,
			kafka_offset INTEGER
		);
		CREATE INDEX IF NOT EXISTS idx_records_vin_txid ON records (vin, txid);
	`)

	if err != nil {
		return nil, fmt.Errorf("error creating records table: %w", err)
	}

	return &SQLiteStore{db: db}, nil
}

// Tx implements datastore.Datastore
func (s *SQLiteStore) Tx(txid string, records []*messages.SocketMessage) (datastore.Datastore, error) {
	return s, nil
}

// StageRecord implements datastore.Datastore
func (s *SQLiteStore) StageRecord(record *datastore.Record) error {
	_, err := s.db.Exec("INSERT INTO records (txid, vin, type, json_data, topic, partition, kafka_offset) VALUES (?, ?, ?, ?, ?, ?, ?)", record.Txid, record.Vin, record.Type, record.JSONData, record.Topic, record.Partition, record.KafkaOffset)
	return err
}

// CommitTx implements datastore.Datastore
func (s *SQLiteStore) CommitTx() error {
	return nil
}

// RollbackTx implements datastore.Datastore
func (s *SQLiteStore) RollbackTx() error {
	return nil
}

// CreateApplicationRecord implements datastore.Datastore
func (s *SQLiteStore) CreateApplicationRecord(record *datastore.ApplicationRecord) error {
	return fmt.Errorf("not implemented")
}

// LoadRecords implements datastore.Datastore
func (s *SQLiteStore) LoadRecords(txid string, batchTopic string) ([]*datastore.Record, error) {
	return nil, fmt.Errorf("not implemented")
}

// UpdateRecordCounts implements datastore.Datastore
func (s *SQLiteStore) UpdateRecordCounts(txid string, numRecords int, numreliableRecords int) error {
	return fmt.Errorf("not implemented")
}

// ReportError implements datastore.Datastore
func (s *SQLiteStore) ReportError(err error, metadata map[string]string, tags []string) {
	airbrake.Notify(err, metadata, tags)
}

// Health implements datastore.Datastore
func (s *SQLiteStore) Health() map[string]string {
	err := s.db.Ping()
	if err != nil {
		return map[string]string{"sqlite": "error", "error_message": err.Error()}
	}

	return map[string]string{"sqlite": "ok"}
}

// CurrentTime implements datastore.Datastore
func (s *SQLiteStore) CurrentTime() time.Time {
	var currentTime time.Time
	row := s.db.QueryRow("SELECT CURRENT_TIMESTAMP")
	err := row.Scan(&currentTime)
	if err != nil {
		s.ReportError(fmt.Errorf("current time from db: %w", err), nil, nil)
		return time.Now().UTC()
	}
	return currentTime
}

// Close implements datastore.Datastore
func (s *SQLiteStore) Close() {
	s.db.Close()
}

// RegisterTxHandler implements datastore.Datastore
func (s *SQLiteStore) RegisterTxHandler(datastore.TxHandler) {
	// Not needed for SQLite as transactions are handled differently
}
