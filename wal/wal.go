// Package wal provides persistent Write-Ahead Log storage for Raft log entries.
// It depends only on the goraft/types package, keeping the dependency graph clean.
package wal

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path"

	"github.com/lwwgo/goraft/types"
)

// WAL is the Write-Ahead Log that persists Raft log entries to disk.
type WAL struct {
	Term     uint64
	Index    uint64
	WorkPath string
	FilePath string
}

// New creates a new WAL instance.
func New(term, index uint64, workPath string) *WAL {
	return &WAL{
		Term:     term,
		Index:    index,
		WorkPath: workPath,
		FilePath: path.Join(workPath, fmt.Sprintf("%016x-%016x.wal", term, index)),
	}
}

// SetPath updates FilePath based on current Term and Index.
func (p *WAL) SetPath() {
	p.FilePath = path.Join(p.WorkPath, fmt.Sprintf("%016x-%016x.wal", p.Term, p.Index))
}

// Append appends a log entry to the WAL file.
func (p *WAL) Append(logEntry *types.LogEntry) error {
	file, err := os.OpenFile(p.FilePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("open file failed, err:%s\n", err.Error())
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(logEntry); err != nil {
		log.Printf("encode json file failed, err:%s\n", err.Error())
		return err
	}
	return nil
}

// Load reads all log entries from a WAL file, returning only those
// with index greater than startIndex.
func (p *WAL) Load(filePath string, startIndex uint64) ([]types.LogEntry, error) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		log.Printf("open file failed, err:%s\n", err.Error())
		return nil, err
	}
	defer file.Close()

	var LogEntries []types.LogEntry
	decoder := json.NewDecoder(file)
	logEntry := &types.LogEntry{}
	for err == nil {
		err = decoder.Decode(logEntry)
		if err == nil && logEntry.Index > startIndex {
			LogEntries = append(LogEntries, *logEntry)
			log.Printf("decode result: %+v\n", *logEntry)
		}
	}

	if err == io.EOF {
		log.Printf("decode json file succ\n")
		return LogEntries, nil
	}

	log.Printf("decode json file failed, file:%s, err:%s\n", p.FilePath, err.Error())
	return nil, err
}
