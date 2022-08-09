package rpc

import (
	"context"
	"errors"
	"fmt"
	"github.com/Lubby-ch/protorpc-v2/codec"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type Call struct {
	Seq           uint64
	ServiceMethod string      // format "<xclient>.<method>"
	Args          interface{} // arguments to the function
	Reply         interface{} // reply from the function
	Error         error       // if error occurs, it will be set
	Done          chan *Call  // Strobes when call is complete.
}

func (call *Call) done() {
	call.Done <- call
}

type Client struct {
	codec ClientCodec

	opt      *Option
	sending  sync.Mutex // protect following
	mu       sync.Mutex // protect following
	seq      uint64
	pending  map[uint64]*Call
	closing  bool // user has called Close
	shutdown bool // server has told us to stop
}

var ErrShutdown = errors.New("connection is shut down")

// Close the connection
func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing {
		return ErrShutdown
	}
	client.closing = true
	return client.codec.Close()
}

// IsAvailable return true if the client does work
func (client *Client) IsAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.shutdown && !client.closing
}

func (client *Client) registerCall(call *Call) error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if !client.IsAvailable() {
		return ErrShutdown
	}

	client.seq++
	call.Seq = client.seq
	client.pending[client.seq] = call
	return nil
}

func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

func (client *Client) terminateCalls(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()
	client.shutdown = true
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
}

func (client *Client) receive() {
	var err error
	for err == nil {
		header := &codec.Response{}
		if err = client.codec.ReadResponseHeader(header); err != nil {
			break
		}

		call := client.removeCall(header.Seq)
		switch {
		case call == nil:
			// it usually means that Write partially failed
			// and call was already removed.
			err = client.codec.ReadResponseBody(nil)
		case header.Error != "":
			call.Error = fmt.Errorf(header.Error)
			err = client.codec.ReadResponseBody(nil)
			call.done()
		default:
			err = client.codec.ReadResponseBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}
	client.terminateCalls(err)
}

func (client *Client) send(call *Call) {
	client.mu.Lock()
	defer client.mu.Unlock()

	err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	err = client.codec.WriteRequest(&codec.Request{
		Seq:           call.Seq,
		ServiceMethod: call.ServiceMethod,
		Opt:           client.opt,
	}, call.Reply)
	if err != nil {
		call.Error = err
		call.done()
		return
	}
}

func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	client.send(call)
	return call
}

func (client *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	call := client.Go(serviceMethod, args, reply, make(chan *Call, 1))
	select {
	case call := <-call.Done:
		return call.Error
	case <-ctx.Done():
		client.removeCall(call.Seq)
		return errors.New("rpc client: call failed: " + ctx.Err().Error())
	}
}

func parseOptions(opts ...*Option) (*Option, error) {
	// if opts is nil or pass nil as parameter
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	return opt, nil
}

func Dial(network, address string, opts ...*Option) (client *Client, err error) {
	return dailTimeout(network, address, opts...)
}

func dailTimeout(network, address string, opts ...*Option) (client *Client, err error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout(network, address, time.Duration(opt.ConnectTimeout))
	if err != nil {
		return nil, err
	}
	defer func() {
		if client == nil {
			_ = conn.Close()
		}
	}()
	return NewClient(conn, opt), nil
}

func NewClient(conn io.ReadWriteCloser, opt *Option) *Client {
	cc := NewClientCodec(conn)
	return NewClientWithCodec(cc, opt)
}

func NewClientWithCodec(cc ClientCodec, opt *Option) *Client {
	client := &Client{
		codec:   cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}
	go client.receive()
	return client
}
