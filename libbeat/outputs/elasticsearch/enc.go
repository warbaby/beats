package elasticsearch

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/elastic/beats/libbeat/common"
)

type bodyEncoder interface {
	bulkBodyEncoder
	Reader() io.Reader
	Marshal(doc interface{}) error
}

type bulkBodyEncoder interface {
	bulkWriter

	AddHeader(*http.Header)
	Reset()
}

type bulkWriter interface {
	Add(meta, obj interface{}) error
	AddRaw(raw interface{}) error
}

type jsonEncoder struct {
	buf *bytes.Buffer
}

type gzipEncoder struct {
	buf  *bytes.Buffer
	gzip *gzip.Writer
}

func newJSONEncoder(buf *bytes.Buffer) *jsonEncoder {
	if buf == nil {
		buf = bytes.NewBuffer(nil)
	}
	return &jsonEncoder{buf}
}

func (b *jsonEncoder) Reset() {
	b.buf.Reset()
}

func (b *jsonEncoder) AddHeader(header *http.Header) {
	header.Add("Content-Type", "application/json; charset=UTF-8")
}

func (b *jsonEncoder) Reader() io.Reader {
	return b.buf
}

func (b *jsonEncoder) Marshal(obj interface{}) error {
	b.Reset()
	enc := json.NewEncoder(b.buf)
	return enc.Encode(obj)
}

func (b *jsonEncoder) AddRaw(raw interface{}) error {
	enc := json.NewEncoder(b.buf)
	return enc.Encode(raw)
}

func (b *jsonEncoder) Add(meta, obj interface{}) error {
	enc := json.NewEncoder(b.buf)
	pos := b.buf.Len()

	if err := enc.Encode(meta); err != nil {
		b.buf.Truncate(pos)
		return err
	}
	if err := enc.Encode(obj); err != nil {
		b.buf.Truncate(pos)
		return err
	}
	return nil
}

func newGzipEncoder(level int, buf *bytes.Buffer) (*gzipEncoder, error) {
	if buf == nil {
		buf = bytes.NewBuffer(nil)
	}
	w, err := gzip.NewWriterLevel(buf, level)
	if err != nil {
		return nil, err
	}

	return &gzipEncoder{buf, w}, nil
}

func (b *gzipEncoder) Reset() {
	b.buf.Reset()
	b.gzip.Reset(b.buf)
}

func (b *gzipEncoder) Reader() io.Reader {
	b.gzip.Close()
	return b.buf
}

func (b *gzipEncoder) AddHeader(header *http.Header) {
	header.Add("Content-Type", "application/json; charset=UTF-8")
	header.Add("Content-Encoding", "gzip")
}

func (b *gzipEncoder) Marshal(obj interface{}) error {
	b.Reset()
	enc := json.NewEncoder(b.gzip)
	err := enc.Encode(obj)
	return err
}

func (b *gzipEncoder) AddRaw(raw interface{}) error {
	enc := json.NewEncoder(b.gzip)
	return enc.Encode(raw)
}

func (b *gzipEncoder) Add(meta, obj interface{}) error {
	enc := json.NewEncoder(b.gzip)
	pos := b.buf.Len()

	if err := enc.Encode(meta); err != nil {
		b.buf.Truncate(pos)
		return err
	}
	if err := enc.Encode(obj); err != nil {
		b.buf.Truncate(pos)
		return err
	}

	b.gzip.Flush()
	return nil
}

type directJsonEncoder struct {
	jsonEncoder
}

func newDirectJSONEncoder(buf *bytes.Buffer) *directJsonEncoder {
	if buf == nil {
		buf = bytes.NewBuffer(nil)
	}
	encoder := directJsonEncoder{}
	encoder.buf = buf
	return &encoder //directJsonEncoder{jsonEncoder{buf}}
}

func isDirectFlagSet(obj interface{}) (bool, interface{}) {
	if amap, ok := obj.(common.MapStr); ok {
		_, direct := amap["send_direct_flag"]
		if direct {
			return direct, amap["message"]
		} else {
			return false, nil
		}
	} else {
		return false, nil
	}
}

// used by bulkEncodePublishRequest
func (b *directJsonEncoder) Add(meta, obj interface{}) error {
	enc := json.NewEncoder(b.buf)
	pos := b.buf.Len()

	if err := enc.Encode(meta); err != nil {
		b.buf.Truncate(pos)
		return err
	}

	//if obj is map and have flag send_direct then send message field directly
	//otherwise fallback to jsonEncoder's Add
	direct, message := isDirectFlagSet(obj)
	if direct {
		if message != nil {
			b.buf.WriteString(message.(string))
			b.buf.WriteByte('\n')
		} else {
			b.buf.Truncate(pos)
			return errors.New("no 'message' field in object")
		}
	} else {
		if err := enc.Encode(obj); err != nil {
			b.buf.Truncate(pos)
			return err
		}
	}

	return nil
}

// used by Connection.request which is called by client.Index and client.Ingest
func (b *directJsonEncoder) Marshal(obj interface{}) error {
	b.Reset()

	direct, message := isDirectFlagSet(obj)
	if direct {
		if message != nil {
			b.buf.WriteString(message.(string))
			b.buf.WriteByte('\n')
			return nil
		} else {
			return errors.New("no 'message' field in object")
		}
	} else {
		enc := json.NewEncoder(b.buf)
		return enc.Encode(obj)
	}
}
