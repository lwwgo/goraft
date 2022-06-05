package util

import (
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"time"
)

var (
	RpcTimeoutErr = errors.New("rpc client call timeout")
)

func RpcCallTimeout(addr, serviceMethod string, args any, reply any, timeout time.Duration) error {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		log.Printf("build rpc client failed, err:%s\n", err.Error())
		return err
	}
	defer client.Close()

	done := make(chan struct{}, 1)
	fail := make(chan struct{}, 1)
	go func() {
		log.Printf("inParams before call args:%+v, reply:%+v\n", args, reply)
		err = client.Call(serviceMethod, args, reply)
		if err != nil {
			fail <- struct{}{}
			return
		}
		done <- struct{}{}
	}()

	if timeout == -1 {
		select {
		case <-done:
			return nil
		case <-fail:
			return err
		}
	} else {
		select {
		case <-time.After(timeout):
			log.Printf("timeout, outParams after call args:%+v, reply:%+v\n", args, reply)
			return RpcTimeoutErr
		case <-done:
			log.Printf("done, outParams after call args:%+v, reply:%+v\n", args, reply)
			return nil
		case <-fail:
			log.Printf("fail, outParams after call args:%+v, reply:%+v\n", args, reply)
			return err
		}
	}
}

func Min[T int | int64 | uint64 | float64](a, b T) T {
	if a < b {
		return a
	}
	return b
}

func Max[T int | int64 | uint64 | float64](a, b T) T {
	if a > b {
		return a
	}
	return b
}

func PathIsExist(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		if os.IsNotExist(err) {
			return false
		}
		fmt.Println(err)
		return false
	}
	return true
}
