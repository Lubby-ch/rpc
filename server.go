package rpc

import (
	"errors"
	"fmt"
	"github.com/Lubby-ch/protorpc-v2/codec"
	"io"
	"log"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"
)

type Request struct {
	req          *codec.Request // header of request
	argv, replyv reflect.Value  // argv and replyv of request
	mtype        *methodType
	svc          *service
	err          error
}

type Server struct {
	serviceMap sync.Map
}

var DefaultServer = &Server{}

func (server *Server) Accept(lis net.Listener) error {
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Println("rpc server: accept error:", err)
			return err
		}

		go server.ServerConn(conn)
	}
}

func (server *Server) ServerConn(conn io.ReadWriteCloser) {
	server.serveCodec(NewServerCodec(conn))
}

func (server *Server) serveCodec(cc ServerCodec) {
	sending := new(sync.Mutex) // make sure to send a complete response
	wg := new(sync.WaitGroup)  // wait until all request are handled
	for {
		req := new(codec.Request)
		err := cc.ReadRequestHeader(req)
		if err != nil {
			if req == nil {
				break // it's not possible to recover, so close the connection
			}
			continue
		}
		var handleReq = &Request{
			req: req,
		}
		handleReq.svc, handleReq.mtype, err = server.findService(req.ServiceMethod)
		if err != nil {
			server.sendRequest(cc, handleReq, sending)
			break
		}

		handleReq.argv = handleReq.mtype.newArgv()
		handleReq.replyv = handleReq.mtype.newReplyv()

		argvi := handleReq.argv.Interface()
		if handleReq.argv.Type().Kind() != reflect.Ptr {
			argvi = handleReq.argv.Addr().Interface()
		}
		if err = cc.ReadRequestBody(argvi); err != nil {
			log.Println("rpc server: read body err:", err)
			break
		}

		wg.Add(1)
		go server.handleRequest(cc, handleReq, sending, wg, req.Opt.HandleTimeout)
	}
	wg.Wait()
	_ = cc.Close()
}

func (server *Server) handleRequest(cc ServerCodec, r *Request, sending *sync.Mutex, wg *sync.WaitGroup, timeout int64) {
	defer wg.Done()
	callch := make(chan struct{})
	sentch := make(chan struct{})
	go func() {
		r.err = r.svc.call(r.mtype, r.argv, r.replyv)
		callch <- struct{}{}
		server.sendRequest(cc, r, sending)
		sentch <- struct{}{}
	}()

	if timeout == 0 {
		<-callch
		<-sentch
		return
	}

	select {
	case <-time.After(time.Second*time.Duration(timeout)):
		r.err = fmt.Errorf("rpc server: request handle timeout: expect within %s", timeout)
		server.sendRequest(cc, r, sending)
	case <-callch:
		select {
		case <-time.After(time.Second*time.Duration(timeout)):
			r.err = fmt.Errorf("rpc server: reponse write timeout: expect within %s", timeout)
			server.sendRequest(cc, r, sending)
		case <-sentch:
		}
	}
}

func (server *Server) sendRequest(cc ServerCodec, r *Request, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	err := r.err
	if err != nil {
		err = cc.WriteResponse(&codec.Response{
			ServiceMethod: r.req.ServiceMethod,
			Seq:           r.req.Seq,
			Error:         err.Error(),
		}, nil)
		return
	}
	err = cc.WriteResponse(&codec.Response{
		ServiceMethod: r.req.ServiceMethod,
		Seq:           r.req.Seq,
	}, r.replyv.Interface())
	if err != nil {
		log.Println("rpc server: handle request error:", err)
	}
}

func (server *Server) Register(i interface{}) error {
	s := newService(i)
	if _, loaded := server.serviceMap.LoadOrStore(s.name, s); loaded {
		return fmt.Errorf("rpc : the xclient %s is already registered", s.name)
	}
	return nil
}

func (server *Server) findService(serviceMethod string) (svc *service, mtype *methodType, err error) {
	strs := strings.Split(serviceMethod, ".")
	if len(strs) != 2 {
		err = errors.New("rpc server: xclient/method request ill-formed: " + serviceMethod)
		return
	}
	svcInter, ok := server.serviceMap.Load(strs[0])
	if !ok {
		err = errors.New("rpc server: can't find xclient " + strs[0])
		return
	}
	svc = svcInter.(*service)
	mtype = svc.method[strs[1]]
	if mtype == nil {
		err = fmt.Errorf("rpc server: xclient %s can't find method %s ", strs[0], serviceMethod)
	}
	return
}

func NewServer() *Server {
	return &Server{}
}

func Accept(lis net.Listener) { DefaultServer.Accept(lis) }

func Register(i interface{}) error {
	return DefaultServer.Register(i)
}
