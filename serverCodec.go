package rpc

import (
	"fmt"
	"github.com/Lubby-ch/protorpc-v2/codec"
	"github.com/Lubby-ch/protorpc-v2/wire"
	"github.com/golang/protobuf/proto"
	"io"
	"sync"
)

type ServerCodec interface {
	WriteResponse(*codec.Response, interface{}) error
	ReadRequestHeader(*codec.Request) error
	ReadRequestBody(interface{}) error

	Close() error
}

type serverCodec struct {
	conn io.ReadWriteCloser

	header  *wire.RequestHeader
	mutex   sync.Mutex
	pending map[uint64]uint64
	seq     uint64
}

func (s *serverCodec) WriteResponse(response *codec.Response, i interface{}) error {
	var resp proto.Message
	if i != nil {
		var ok bool
		resp, ok = i.(proto.Message)
		if !ok {
			if _, ok = i.(struct{}); !ok {
				s.mutex.Lock()
				delete(s.pending, response.Seq)
				s.mutex.Unlock()
			}
			return fmt.Errorf("rpc.ServerCodec.WriteResponse: %T does not implement proto.Message", i)
		}
	}
	s.mutex.Lock()
	id, ok := s.pending[response.Seq]
	if !ok {
		s.mutex.Unlock()
		return fmt.Errorf("rpc: invalid sequence number in response")
	}
	s.mutex.Unlock()
	return writeResponse(s.conn, id, response.Error, resp)
}

func (s *serverCodec) ReadRequestHeader(request *codec.Request) error {
	header := new(wire.RequestHeader)
	header.Opt = new(wire.Option)
	err := readRequestHeader(s.conn, header)
	if err != nil {
		return err
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.header = header

	s.seq++
	s.pending[s.seq] = header.Id
	request.ServiceMethod = header.Method
	request.Seq = s.seq
	request.Opt = &Option{
		MagicNumber:    header.Opt.MagicNumber,
		HandleTimeout:  header.Opt.HandleTimeout,
		ConnectTimeout: header.Opt.ConnectTimeout,
	}
	return nil
}

func (s *serverCodec) ReadRequestBody(i interface{}) error {
	body, ok := i.(proto.Message)
	if !ok {
		return fmt.Errorf("rpc.ServerCodec.ReadRequestBody: %T does not implement proto.Message", i)
	}
	err := readRequestBody(s.conn, s.header, body)
	if err != nil {
		return err
	}
	s.header.Reset()
	return nil
}

func NewServerCodec(conn io.ReadWriteCloser) ServerCodec {
	return &serverCodec{
		pending: make(map[uint64]uint64),
		conn:    conn,
	}
}

func (s *serverCodec) Close() error {
	return s.conn.Close()
}
