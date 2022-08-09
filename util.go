package rpc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/Lubby-ch/protorpc-v2/wire"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"go/ast"
	"hash/crc32"
	"io"
	"net"
	"reflect"
)

var (
	UseSnappy            = true
	UseCrc32ChecksumIEEE = true
)

const (
	CONST_REQUEST_HEADER_MAX_LEN = 1024
	CONST_UINT64_BYTE_NUM        = 8
)

func writeRequest(writer io.Writer, id uint64, method string, opt *Option, request proto.Message) (err error) {
	var (
		protoReq         = []byte{}
		comoressProtoReq = []byte{}
	)

	if request != nil {
		protoReq, err = proto.Marshal(request)
		if err != nil {
			return err
		}
	}

	comoressProtoReq = snappy.Encode(nil, protoReq)

	header := &wire.RequestHeader{
		Id:                         id,
		Method:                     method,
		RawRequestLen:              uint32(len(protoReq)),
		SnappyCompressedRequestLen: uint32(len(comoressProtoReq)),
		Checksum:                   crc32.ChecksumIEEE(comoressProtoReq),

		Opt: &wire.Option{
			MagicNumber:    opt.MagicNumber,
			HandleTimeout:  opt.HandleTimeout,
			ConnectTimeout: opt.ConnectTimeout,
		},
	}

	if !UseSnappy || header.RawRequestLen < header.SnappyCompressedRequestLen {
		header.SnappyCompressedRequestLen = 0
		comoressProtoReq = protoReq
	}

	if !UseCrc32ChecksumIEEE {
		header.Checksum = 0
	} else {
		header.Checksum = crc32.ChecksumIEEE(comoressProtoReq)
	}

	protoHeader, err := proto.Marshal(header)
	if err != nil {
		return err
	}

	if len(protoHeader) > CONST_REQUEST_HEADER_MAX_LEN {
		return fmt.Errorf("rpc.writeRequest: the header length: %d is larger than the limit of header : %d.", len(protoHeader), CONST_REQUEST_HEADER_MAX_LEN)
	}

	if err = Send(writer, protoHeader); err != nil {
		return nil
	}
	return Send(writer, comoressProtoReq)
}

func readRequestHeader(reader io.Reader, header proto.Message) (err error) {
	protoHeader, err := Recv(reader, 1024)
	if err != nil {
		return err
	}
	return proto.Unmarshal(protoHeader, header)
}

func readRequestBody(reader io.Reader, header *wire.RequestHeader, body proto.Message) (err error) {
	maxBodyLen := MaxUint32(header.RawRequestLen, header.SnappyCompressedRequestLen)

	compressProtoBody, err := Recv(reader, uint64(maxBodyLen))
	if err != nil {
		return err
	}

	if header.Checksum != 0 {
		if crc32.ChecksumIEEE(compressProtoBody) != header.Checksum {
			return fmt.Errorf("rpc.readRequestBody: checksum err ")
		}
	}

	var protoBody = compressProtoBody
	if header.SnappyCompressedRequestLen != 0 {
		// 解压
		protoBody, err = snappy.Decode(nil, compressProtoBody)
		if err != nil {
			return err
		}
		// check wire header: rawMsgLen
		if uint32(len(protoBody)) != header.RawRequestLen {
			return fmt.Errorf("rpc.readRequestBody: Unexcpeted header.RawResponseLen.")
		}
	}

	if body != nil {
		err = proto.Unmarshal(protoBody, body)
		if err != nil {
			return nil
		}
	}
	return nil
}

func writeResponse(writer io.Writer, id uint64, strErr string, response proto.Message) (err error) {
	if strErr != "" {
		response = nil
	}

	var (
		protoResp         = []byte{}
		compressProtoResp = []byte{}
	)
	if response != nil {
		protoResp, err = proto.Marshal(response)
		if err != nil {
			return err
		}
	}

	compressProtoResp = snappy.Encode(nil, protoResp)

	header := &wire.ResponseHeader{
		Id:                          id,
		Error:                       strErr,
		RawResponseLen:              uint32(len(protoResp)),
		SnappyCompressedResponseLen: uint32(len(compressProtoResp)),
	}

	if !UseSnappy {
		header.SnappyCompressedResponseLen = 0
		compressProtoResp = protoResp
	}
	if !UseCrc32ChecksumIEEE {
		header.Checksum = 0
	} else {
		header.Checksum = crc32.ChecksumIEEE(compressProtoResp)
	}

	protoHeader, err := proto.Marshal(header)
	if err != nil {
		return nil
	}
	if len(protoHeader) > CONST_REQUEST_HEADER_MAX_LEN {
		return fmt.Errorf("rpc.writeResponse: the header length: %d is larger than the limit of header : %d.", len(protoHeader), CONST_REQUEST_HEADER_MAX_LEN)
	}
	if err = Send(writer, protoHeader); err != nil {
		return nil
	}

	return Send(writer, compressProtoResp)
}

func readResponseHeader(reader io.Reader, header proto.Message) (err error) {
	protoHeader, err := Recv(reader, 1024)
	if err != nil {
		return err
	}
	return proto.Unmarshal(protoHeader, header)
}

func readResponseBody(reader io.Reader, header *wire.ResponseHeader, body proto.Message) (err error) {
	maxBodyLen := MaxUint32(header.RawResponseLen, header.SnappyCompressedResponseLen)

	compressProtoBody, err := Recv(reader, uint64(maxBodyLen))
	if err != nil {
		return err
	}

	if header.Checksum != 0 {
		if crc32.ChecksumIEEE(compressProtoBody) != header.Checksum {
			return fmt.Errorf("rpc.readRequestBody: checksum err ")
		}
	}

	var protoBody = compressProtoBody
	if header.SnappyCompressedResponseLen != 0 {
		protoBody, err = snappy.Decode(nil, compressProtoBody)
		if err != nil {
			return err
		}
		// check wire header: rawMsgLen
		if uint32(len(protoBody)) != header.RawResponseLen {
			return fmt.Errorf("rpc.readResponseBody: Unexcpeted header.RawResponseLen.")
		}
	}

	if body != nil {
		err = proto.Unmarshal(protoBody, body)
		if err != nil {
			return nil
		}
	}
	return nil
}

func Recv(reader io.Reader, maxSize uint64) (data []byte, err error) {
	size, err := ReadSize(reader)
	if err != nil {
		return nil, err
	}
	if maxSize > 0 && size > maxSize {
		return nil, fmt.Errorf("rpc: data size overflows maxSize(%d)", maxSize)
	}
	if size == 0 {
		return
	}
	data = make([]byte, size)
	if err = Read(reader, data); err != nil {
		return nil, err
	}
	return data, nil
}

func Send(writer io.Writer, data []byte) (err error) {
	var size [binary.MaxVarintLen64]byte
	if data == nil || len(data) == 0 {
		n := binary.PutUvarint(size[:], uint64(0))
		return Write(writer, size[:n], false)
	}

	n := binary.PutUvarint(size[:], uint64(len(data)))
	err = Write(writer, size[:n], false)
	if err != nil {
		return err
	}
	return Write(writer, data, false)
}

func ReadSize(reader io.Reader) (uint64, error) {
	var (
		size  uint64
		shift uint64
	)
	for i := 1; ; i++ {
		data, err := ReadByte(reader)
		if err != nil {
			return 0, err
		}
		if data < 0x80 {
			if i == CONST_UINT64_BYTE_NUM && data&0x80 > 0 {
				return 0, errors.New("rpc: header size overflows a 64-bit integer")
			}
			return size | uint64(data)<<shift, nil
		}

		size |= uint64(data&0x7F) << shift
		shift += 7
	}
}

func ReadByte(reader io.Reader) (byte, error) {
	buff := make([]byte, 1)
	if err := Read(reader, buff); err != nil {
		return 0, err
	}
	return buff[0], nil
}

func Read(reader io.Reader, buff []byte) error {
	for i := 0; i < len(buff); {
		n, err := reader.Read(buff[i:])
		if err != nil {
			if nerr, ok := err.(net.Error); !ok || !nerr.Temporary() {
				return err
			}
		}
		i += n
	}
	return nil
}

func Write(writer io.Writer, data []byte, onePacket bool) error {
	if onePacket {
		if _, err := writer.Write(data); err != nil {
			return nil
		}
		return nil
	}
	for i := 0; i < len(data); {
		n, err := writer.Write(data[i:])
		if err != nil {
			if nerr, ok := err.(net.Error); !ok || !nerr.Temporary() {
				return err
			}
		}
		i += n
	}
	return nil
}

func MaxUint32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}

func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}

const MagicNumber = 0x3bef5c

type Option struct {
	MagicNumber    int64 // MagicNumber marks this's a geerpc request
	ConnectTimeout int64 // 0 means no limit
	HandleTimeout  int64
}

var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	ConnectTimeout: 10,
}
