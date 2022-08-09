package codec

import rpc "github.com/Lubby-ch/protorpc-v2"

// Request is a header written before every RPC call. It is used internally
// but documented here as an aid to debugging, such as when analyzing
// network traffic.
type Request struct {
	ServiceMethod string // format: "Service.Method"
	Seq           uint64 // sequence number chosen by client
	Opt           *rpc.Option
}

// Response is a header written before every RPC return. It is used internally
// but documented here as an aid to debugging, such as when analyzing
// network traffic.
type Response struct {
	ServiceMethod string // echoes that of the Request
	Seq           uint64 // echoes that of the request
	Error         string // error, if any.
}
