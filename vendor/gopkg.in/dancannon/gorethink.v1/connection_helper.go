package gorethink

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	p "gopkg.in/dancannon/gorethink.v1/ql2"
)

// Write 'data' to conn
func (c *Connection) writeData(data []byte) error {
	_, err := c.Conn.Write(data[:])
	if err != nil {
		return err
	}

	return nil
}

func (c *Connection) writeHandshakeReq() error {
	pos := 0
	dataLen := 4 + 4 + len(c.opts.AuthKey) + 4
	data := make([]byte, dataLen)

	// Send the protocol version to the server as a 4-byte little-endian-encoded integer
	binary.LittleEndian.PutUint32(data[pos:], uint32(p.VersionDummy_V0_4))
	pos += 4

	// Send the length of the auth key to the server as a 4-byte little-endian-encoded integer
	binary.LittleEndian.PutUint32(data[pos:], uint32(len(c.opts.AuthKey)))
	pos += 4

	// Send the auth key as an ASCII string
	if len(c.opts.AuthKey) > 0 {
		pos += copy(data[pos:], c.opts.AuthKey)
	}

	// Send the protocol type as a 4-byte little-endian-encoded integer
	binary.LittleEndian.PutUint32(data[pos:], uint32(p.VersionDummy_JSON))
	pos += 4

	return c.writeData(data)
}

func (c *Connection) readHandshakeSuccess() error {
	reader := bufio.NewReader(c.Conn)
	line, err := reader.ReadBytes('\x00')
	if err != nil {
		if err == io.EOF {
			return fmt.Errorf("Unexpected EOF: %s", string(line))
		}
		return err
	}
	// convert to string and remove trailing NUL byte
	response := string(line[:len(line)-1])
	if response != "SUCCESS" {
		response = strings.TrimSpace(response)
		// we failed authorization or something else terrible happened
		return RQLDriverError{rqlError(fmt.Sprintf("Server dropped connection with message: \"%s\"", response))}
	}

	return nil
}

func (c *Connection) read(buf []byte, length int) (total int, err error) {
	var n int
	for total < length {
		if n, err = c.Conn.Read(buf[total:length]); err != nil {
			break
		}
		total += n
	}
	if err != nil {
		return total, err
	}

	return total, nil
}

func (c *Connection) writeQuery(token int64, q []byte) error {
	pos := 0
	dataLen := 8 + 4 + len(q)
	data := make([]byte, dataLen)

	// Send the protocol version to the server as a 4-byte little-endian-encoded integer
	binary.LittleEndian.PutUint64(data[pos:], uint64(token))
	pos += 8

	// Send the length of the auth key to the server as a 4-byte little-endian-encoded integer
	binary.LittleEndian.PutUint32(data[pos:], uint32(len(q)))
	pos += 4

	// Send the auth key as an ASCII string
	pos += copy(data[pos:], q)

	return c.writeData(data)
}
