package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path"
)

type Persistence struct {
	Term     uint64
	Index    uint64
	WorkPath string
	FilePath string
}

func NewPersistence(term, index uint64, workPath string) *Persistence {
	return &Persistence{
		Term:     term,
		Index:    index,
		WorkPath: workPath,
		FilePath: path.Join(workPath, fmt.Sprintf("%016x-%016x.wal", term, index)),
	}
}

func (p *Persistence) SetPath() {
	p.FilePath = path.Join(p.WorkPath, fmt.Sprintf("%016x-%016x.wal", p.Term, p.Index))
}

// Append 在文件末尾追加写一条raft操作日志
func (p *Persistence) Append(logEntry *LogEntry) error {
	file, err := os.OpenFile(p.FilePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("open file failed, err:%s\n", err.Error())
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(logEntry); err != nil {
		log.Printf("encode json file failed, err:%s\n", err.Error())
		return err
	}
	return nil
}

// Load 加载磁盘文件到内存
func (p *Persistence) Load(filePath string, startIndex uint64) ([]LogEntry, error) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		log.Printf("open file failed, err:%s\n", err.Error())
	}
	defer file.Close()

	var LogEntries []LogEntry
	decoder := json.NewDecoder(file)
	logEntry := &LogEntry{}
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
