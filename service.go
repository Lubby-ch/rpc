package rpc

import (
	"go/ast"
	"log"
	"reflect"
	"sync/atomic"
)

type methodType struct {
	method    reflect.Method
	ArgType   reflect.Type
	ReplyType reflect.Type
	numCalls  uint64
}

func (m *methodType) NumCalls() uint64 {
	return atomic.LoadUint64(&m.numCalls)
}

func (m *methodType) newArgv() reflect.Value {
	var argv reflect.Value
	if m.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(m.ArgType.Elem())
	} else {
		argv = reflect.New(m.ArgType).Elem()
	}
	return argv
}

func (m *methodType) newReplyv() reflect.Value {
	reply := reflect.New(m.ReplyType.Elem())
	switch m.ReplyType.Elem().Kind() {
	case reflect.Map:
		reply.Elem().Set(reflect.MakeMap(m.ReplyType.Elem()))
	case reflect.Slice:
		reply.Elem().Set(reflect.MakeSlice(m.ReplyType.Elem(), 0, 0))
	}
	return reply
}

type service struct {
	name    string
	rtype   reflect.Type
	rvalue reflect.Value
	method map[string]*methodType
}

func newService(i interface{}) *service {
	s := new(service)
	s.rvalue = reflect.ValueOf(i)
	s.rtype = reflect.TypeOf(i)
	s.name = reflect.Indirect(s.rvalue).Type().Name()
	s.method = make(map[string]*methodType)
	if !ast.IsExported(s.name) {
		log.Fatalf("rpc server: %s is not a valid xclient name", s.name)
	}
	s.registerMethods()
	return s
}

func (s *service) registerMethods() {
	for i := 0; i < s.rtype.NumMethod(); i++ {
		method := s.rtype.Method(i)
		mtype := method.Type
		if mtype.NumIn() != 3 || mtype.NumOut() != 1 {
			continue
		}
		if mtype.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		argType, replyType := mtype.In(1), mtype.In(2)
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}
		s.method[method.Name] = &methodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
		log.Printf("rpc server: register %s.%s \n", s.name, method.Name)
	}
}

func (s *service) call(m *methodType, argv, replyv reflect.Value) error {
	atomic.AddUint64(&m.numCalls, 1)
	f := m.method.Func
	returnValues := f.Call([]reflect.Value{s.rvalue, argv, replyv})
	if errIntertface := returnValues[0].Interface(); errIntertface != nil {
		return errIntertface.(error)
	}
	return nil
}
