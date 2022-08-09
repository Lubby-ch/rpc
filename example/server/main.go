package main

import (
	"errors"
	rpc "github.com/Lubby-ch/protorpc-v2"
	"github.com/Lubby-ch/protorpc-v2/example/server/pb"
	"net"
)

// 算数运算结构体
type Arith struct {
}

// 乘法运算方法
func (this *Arith) Multiply(req *pb.ArithRequest, res *pb.ArithResponse) error {
	res.Pro = req.A * req.B
	return nil
}

// 除法运算方法
func (this *Arith) Divide(req *pb.ArithRequest, res *pb.ArithResponse) error {
	if req.B == 0 {
		return errors.New("divide by zero")
	}
	res.Quo = req.A / req.B
	res.Rem = req.A % req.B
	return nil
}

func main() {
	server := rpc.NewServer()
	server.Register(new(Arith))
	lis, err := net.Listen("TCP", "127.0.0.1:10000")
	if err != nil {
		return
	}
	server.Accept(lis)
}
