package server

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
)

type SnapshotMetadata struct {
	Index uint64
	Term  uint64
}

type Snapshot struct {
	Data     []byte
	Metadata SnapshotMetadata
}

type Snapshotter struct {
	StartTerm  uint64
	StartIndex uint64
	EndTerm    uint64
	EndIndex   uint64
	WorkPath   string
	FilePath   string
}

func NewSnap(term, index uint64, workPath string) *Snapshotter {
	return &Snapshotter{
		StartTerm:  term,
		StartIndex: index,
		WorkPath:   workPath,
		FilePath:   path.Join(workPath, fmt.Sprintf("%016x-%016x.snap", term, index)),
	}
}

func (sp *Snapshotter) GetPath() string {
	return sp.FilePath
}

// Save 保存业务层的快照数据到磁盘文件
func (sp *Snapshotter) Save(snapshot *Snapshot) error {
	file, err := os.OpenFile(sp.FilePath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Printf("open file failed, err:%s\n", err.Error())
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(snapshot); err != nil {
		log.Printf("encode json file failed, err:%s\n", err.Error())
		return err
	}
	return nil
}

// Load 加载快照文件到快照的内存结构
func (sp *Snapshotter) Load(filePath string) (*Snapshot, error) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		log.Printf("open file failed, err:%s\n", err.Error())
	}
	defer file.Close()

	var snapshot Snapshot
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&snapshot)
	if err != nil {
		log.Printf("load snapshot file fail, file:%s, err:%s\n", sp.FilePath, err.Error())
		return nil, err
	}

	log.Printf("load snapshot file succ, content: %+v\n", snapshot)
	return &snapshot, nil
}
