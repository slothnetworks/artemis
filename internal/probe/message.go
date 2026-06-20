package probe

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"

	"github.com/slothnetworks/artemis/internal/buffr"
)

// Message represents a connqc message.
type Message interface {
	unexported()
}

// Probe is a probe message.
type Probe struct {
	ID   uint64
	Data string
}

func (p Probe) unexported() {}

// Encoder encodes messages onto a stream.
type Encoder struct {
	w io.Writer
}

// NewEncoder returns an encoder with the given writer.
func NewEncoder(w io.Writer) Encoder {
	return Encoder{w: w}
}

// Encode encodes a message onto the steam.
func (e Encoder) Encode(msg Message) error {
	switch v := msg.(type) {
	case Probe:
		var idBytes [8]byte
		binary.BigEndian.PutUint64(idBytes[:], v.ID)

		dataLen := len(v.Data)
		if dataLen > math.MaxUint16 {
			return errors.New("probe data is too long")
		}

		var lenBytes [2]byte
		binary.BigEndian.PutUint16(lenBytes[:], uint16(dataLen))

		var buf bytes.Buffer
		buf.WriteString("PRB")
		buf.Write(idBytes[:])
		buf.Write(lenBytes[:])
		buf.WriteString(v.Data)

		_, err := e.w.Write(buf.Bytes())
		return err
	default:
		return errors.New("unsupported message type")
	}
}

// Decoder decodes messages from a reader.
type Decoder struct {
	r io.Reader
}

// DefaultDecodeBuffer is the default decode buffer size, large enough to hold a
// full datagram so large / MTU-probing payloads are not truncated on the read
// path.
const DefaultDecodeBuffer = 65535

// NewDecoder returns a decoder for the given reader using DefaultDecodeBuffer.
func NewDecoder(r io.Reader) Decoder {
	return NewDecoderSize(r, DefaultDecodeBuffer)
}

// NewDecoderSize returns a decoder for the given reader with an explicit buffer
// size (the maximum single-read / datagram size it can handle).
func NewDecoderSize(r io.Reader, size int) Decoder {
	if size <= 0 {
		size = DefaultDecodeBuffer
	}
	return Decoder{
		// The buffered reader solves the issue of reading packets at once (required for UDP) while
		// still being able to read byte by byte to verify the input.
		r: buffr.NewReader(r, size),
	}
}

// Decode decodes a message off the stream.
func (d Decoder) Decode() (Message, error) {
	var typ [3]byte
	_, err := io.ReadFull(d.r, typ[:])
	if err != nil {
		return nil, err
	}

	switch string(typ[:]) {
	case "PRB":
		var b [10]byte
		_, err = io.ReadFull(d.r, b[:])
		if err != nil {
			return nil, err
		}

		l := binary.BigEndian.Uint16(b[8:])
		data := make([]byte, l)
		_, err = io.ReadFull(d.r, data)
		if err != nil {
			return nil, err
		}

		return Probe{
			ID:   binary.BigEndian.Uint64(b[:]),
			Data: string(data),
		}, nil
	default:
		return nil, errors.New("unsupported message type")
	}
}
