// Package snapshot provides persistent storage for Raft state machine snapshots.
// It depends only on the goraft/types package, keeping the dependency graph clean.
package snapshot

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"

	"github.com/lwwgo/goraft/types"
)

// Snapshotter handles saving and loading state machine snapshots.
type Snapshotter struct {
	StartTerm  uint64
	StartIndex uint64
	EndTerm    uint64
	EndIndex   uint64
	WorkPath   string
	FilePath   string
}

// New creates a new Snapshotter instance.
func New(term, index uint64, workPath string) *Snapshotter {
	return &Snapshotter{
		StartTerm:  term,
		StartIndex: index,
		WorkPath:   workPath,
		FilePath:   path.Join(workPath, fmt.Sprintf("%016x-%016x.snap", term, index)),
	}
}

// GetPath returns the snapshot file path.
func (sp *Snapshotter) GetPath() string {
	return sp.FilePath
}

// Save persists a snapshot to disk.
func (sp *Snapshotter) Save(snapshot *types.Snapshot) error {
	file, err := os.OpenFile(sp.FilePath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Printf("open file failed, err:%s\n", err.Error())
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(snapshot); err != nil {
		log.Printf("encode json file failed, err:%s\n", err.Error())
		return err
	}
	return nil
}

// Load reads a snapshot from disk.
func (sp *Snapshotter) Load(filePath string) (*types.Snapshot, error) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		log.Printf("open file failed, err:%s\n", err.Error())
		return nil, err
	}
	defer file.Close()

	var snapshot types.Snapshot
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&snapshot)
	if err != nil {
		log.Printf("load snapshot file fail, file:%s, err:%s\n", sp.FilePath, err.Error())
		return nil, err
	}

	log.Printf("load snapshot file succ, content: %+v\n", snapshot)
	return &snapshot, nil
}
