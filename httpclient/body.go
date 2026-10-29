package httpclient

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
)

func readReplayBody(ctx context.Context, getBody func() (io.ReadCloser, error)) ([]byte, error) {
	body, err := getBody()
	if err != nil {
		return nil, err
	}

	if body == nil {
		return nil, errors.New("request GetBody returned a nil body")
	}

	closeBody := sync.OnceValue(body.Close)
	stop := context.AfterFunc(ctx, func() { _ = closeBody() })
	defer stop()

	data, readErr := io.ReadAll(body)
	closeErr := closeBody()

	return data, errors.Join(readErr, closeErr)
}

// Capture streaming uploads as the transport reads them, without pre-draining.
type capturedBody struct {
	source   io.ReadCloser
	expected int64
	mu       sync.Mutex
	data     bytes.Buffer
	readErr  error
	eof      bool
	closed   bool
	emit     func([]byte, error)
	emitted  bool
}

func (b *capturedBody) Read(p []byte) (int, error) {
	n, err := b.source.Read(p)

	b.mu.Lock()
	defer b.mu.Unlock()

	_, _ = b.data.Write(p[:n])
	if err == io.EOF {
		b.eof = true
	} else if err != nil {
		b.readErr = err
	}

	return n, err
}

func (b *capturedBody) Close() error {
	err := b.source.Close()

	b.mu.Lock()
	b.closed = true
	b.readErr = errors.Join(b.readErr, err)
	b.mu.Unlock()
	b.maybeEmit()

	return err
}

func (b *capturedBody) publish(emit func([]byte, error)) {
	b.mu.Lock()
	b.emit = emit
	b.mu.Unlock()
	b.maybeEmit()
}

func (b *capturedBody) maybeEmit() {
	b.mu.Lock()
	if b.emit == nil || !b.closed || b.emitted {
		b.mu.Unlock()
		return
	}

	b.emitted = true
	data := bytes.Clone(b.data.Bytes())
	err := b.readErr
	if err == nil && !b.eof && (b.expected <= 0 || int64(len(data)) != b.expected) {
		err = errors.New("request body was not fully sent; curl unavailable")
	}

	emit := b.emit
	b.mu.Unlock()
	emit(data, err)
}
