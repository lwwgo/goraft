package util

import (
	"errors"
	"log"
	"net/rpc"
	"time"
)

func RpcCallTimeout(client *rpc.Client, serviceMethod string, args any, reply any, timeout time.Duration) error {
	log.Printf("inParams args:%+v, reply:%+v\n", args, reply)
	var err error
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
			return errors.New("rpc client call request timeout")
		case <-done:
			return nil
		case <-fail:
			return err
		}
	}
}
