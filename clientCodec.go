package rpc

import (
	"fmt"
	"github.com/Lubby-ch/protorpc-v2/codec"
	"github.com/Lubby-ch/protorpc-v2/wire"
	"github.com/golang/protobuf/proto"
	"io"
	"sync"
)

type ClientCodec interface {
	WriteRequest(*codec.Request, interface{}) error
	ReadResponseHeader(*codec.Response) error
	ReadResponseBody(interface{}) error

	Close() error
}

type clientCodec struct {
	conn io.ReadWriteCloser

	respHeader *wire.ResponseHeader
	// Protobuf-RPC responses include the request id but not the request method.
	// Package rpc expects both.
	// We save the request method in pending when sending a request
	// and then look it up by request ID when filling out the rpc Response.
	mutex   sync.Mutex // protects seq, pending
	pending map[uint64]string
}

func NewClientCodec(conn io.ReadWriteCloser) ClientCodec {
	return &clientCodec{
		conn:    conn,
		pending: make(map[uint64]string),
	}
}

// WriteRequest handles user's request and send request.Seq and request.ServiceMethod to the remote server to realize
// Remote procedure call.
//@param request
func (c *clientCodec) WriteRequest(request *codec.Request, i interface{}) error {
	c.mutex.Lock()
	c.pending[request.Seq] = request.ServiceMethod
	c.mutex.Unlock()

	var (
		req proto.Message
	)
	if i != nil {
		var ok bool
		req, ok = i.(proto.Message)
		if !ok {
			return fmt.Errorf("rpc.ClientCodec.WriteRequest: %T does not implement proto.Message", i)
		}
	}
	return writeRequest(c.conn, request.Seq, request.ServiceMethod, request.Opt, req)
}

func (c *clientCodec) ReadResponseHeader(response *codec.Response) error {
	var (
		header = new(wire.ResponseHeader)
	)
	err := readResponseHeader(c.conn, header)
	if err != nil {
		return nil
	}

	c.mutex.Lock()
	response.Seq = header.Id
	response.Error = header.Error
	response.ServiceMethod = c.pending[response.Seq] // when can not find Service Method, "" is permitted
	delete(c.pending, response.Seq)
	c.mutex.Unlock()

	c.respHeader = header
	return nil
}

func (c *clientCodec) ReadResponseBody(i interface{}) error {
	var response proto.Message
	if i != nil {
		var ok bool
		response, ok = i.(proto.Message)
		if !ok {
			return fmt.Errorf("rpc.ServerCodec.ReadResponseBody: %T does not implement proto.Message", i)
		}
	}

	err := readResponseBody(c.conn, c.respHeader, response)
	if err != nil {
		return err
	}

	c.respHeader.Reset()
	return nil
}

func (c *clientCodec) Close() error {
	return c.conn.Close()
}
