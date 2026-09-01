package handlers

import (
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
)

type Noop struct{}

func (n Noop) Init() error {
	return nil
}

func (n Noop) Write(msg []byte) error {
	fmt.Println("Received message:", string(msg))
	return nil
}

func (n Noop) Invoke() (*arrow.Table, error) {
	return nil, nil
}
