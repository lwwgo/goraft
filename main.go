package main

import (
	"flag"
	"io"
	"log"
	"os"

	"github.com/lwwgo/goraft/server"
	"github.com/spf13/viper"
)

type Config struct {
	LocalID string   `toml:"localID"`
	Peers   []string `toml:"peers"`
	Learner string   `toml:"learner"`
	WalDir  string   `toml:"walDir"`
	SnapDir string   `toml:"snapDir"`
}

func main() {
	configName := flag.String("c", "conf.toml", "conf")
	flag.Parse()

	f, err := os.OpenFile("log/"+*configName+".log", os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return
	}
	defer f.Close()
	// 日志同时输出到终端和文件
	multiWriter := io.MultiWriter(os.Stdout, f)
	log.SetOutput(multiWriter)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	var config Config
	curPath, err := os.Getwd()
	if err != nil {
		return
	}

	vip := viper.New()
	vip.AddConfigPath(curPath + "/conf")
	vip.SetConfigName(*configName)
	vip.SetConfigType("toml")
	if err := vip.ReadInConfig(); err != nil {
		panic(err)
	}
	if err := vip.Unmarshal(&config); err != nil {
		panic(err)
	}
	log.Printf("config %+v\n", config)

	log.Printf("start raft clueter!\n")
	rfNode, err := server.InitServer(config.LocalID, config.Peers, config.WalDir, config.SnapDir)
	if err != nil {
		panic(err)
	}
	rfNode.Run()

	defer log.Printf("exist raft clueter!\n")
}
