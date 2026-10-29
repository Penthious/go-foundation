package httpclient_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/httpclient"
	"github.com/Penthious/go-foundation/logger"
)

func TestBody_StreamingStartsBeforeCapture(t *testing.T) {
	started := make(chan struct{})
	received := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}

		received <- body
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	reader, writer := io.Pipe()
	t.Cleanup(func() {
		_ = reader.Close()
		_ = writer.Close()
	})
	payload := []byte("streamed request with 'quotes'\n")
	done := make(chan error, 1)
	go func() {
		select {
		case <-started:
			_, err := writer.Write(payload)
			done <- errors.Join(err, writer.Close())
		case <-t.Context().Done():
			done <- t.Context().Err()
		}
	}()
	log, sink := newLogger(t, logger.LevelDebug)
	client := newClient(t, httpclient.Config{Logger: log})

	resp, err := client.Do(newRequest(t, http.MethodPost, server.URL, reader))
	if err != nil {
		t.Fatal(err)
	}

	closeResponse(t, resp)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	if body := <-received; !bytes.Equal(body, payload) {
		t.Errorf("request body = %q, want %q", body, payload)
	}

	record := sink.wait(t)
	if _, ok := record.Fields["curl"].(string); !ok {
		t.Errorf("complete streamed request did not produce curl: %v", record.Fields)
	}
}

func TestBody_CopyFailureDoesNotChangeRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if string(body) != "original body" {
			t.Errorf("request body changed: %q", body)
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	log, sink := newLogger(t, logger.LevelDebug)
	client := newClient(t, httpclient.Config{Logger: log})
	req := newRequest(t, http.MethodPost, server.URL, strings.NewReader("original body"))
	req.GetBody = func() (io.ReadCloser, error) {
		return nil, errors.New("copy unavailable")
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	closeResponse(t, resp)
	record := sink.wait(t)
	if record.Fields["curl"] != nil || record.Fields["curl_error"] != "copy unavailable" {
		t.Errorf("expected an explicit curl error: %v", record.Fields)
	}
}

func TestBody_InterruptedUploadDoesNotProducePartialCurl(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
	}))
	defer server.Close()
	log, sink := newLogger(t, logger.LevelDebug)
	client := newClient(t, httpclient.Config{Logger: log})
	req := newRequest(t, http.MethodPost, server.URL, failingBody{})

	resp, err := client.Do(req)
	if resp != nil {
		closeResponse(t, resp)
	}

	if err == nil {
		t.Fatal("broken upload unexpectedly succeeded")
	}

	record := sink.wait(t)
	if record.Fields["curl"] != nil || record.Fields["curl_error"] == nil {
		t.Errorf("interrupted upload was presented as replayable: %v", record.Fields)
	}
}

func TestBody_CanceledReplayCopyClosesOnce(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	log, sink := newLogger(t, logger.LevelDebug)
	client := newClient(t, httpclient.Config{Logger: log, Timeout: 100 * time.Millisecond})
	copy := &blockingBody{done: make(chan struct{})}
	req := newRequest(t, http.MethodPost, server.URL, strings.NewReader("original"))
	req.GetBody = func() (io.ReadCloser, error) {
		return copy, nil
	}

	resp, err := client.Do(req)
	if resp != nil {
		closeResponse(t, resp)
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Do error = %v, want timeout", err)
	}

	sink.wait(t)
	if copy.closes.Load() != 1 {
		t.Errorf("replay copy was closed %d times, want 1", copy.closes.Load())
	}
}

type blockingBody struct {
	done   chan struct{}
	once   sync.Once
	closes atomic.Int32
}

func (b *blockingBody) Read([]byte) (int, error) {
	<-b.done
	return 0, io.ErrClosedPipe
}

func (b *blockingBody) Close() error {
	b.closes.Add(1)
	b.once.Do(func() { close(b.done) })

	return nil
}

type failingBody struct{}

func (failingBody) Read(p []byte) (int, error) {
	return copy(p, "partial"), errors.New("upload interrupted")
}

func (failingBody) Close() error {
	return nil
}
