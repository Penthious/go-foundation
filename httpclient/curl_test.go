package httpclient_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/httpclient"
	"github.com/Penthious/go-foundation/logger"
)

func TestCurl_ReplaysRequest(t *testing.T) {
	if _, err := exec.LookPath("curl"); err != nil {
		t.Skip("curl is required for replay verification")
	}

	tests := []struct {
		name   string
		method string
		body   []byte
	}{
		{name: "GET", method: http.MethodGet},
		{name: "JSON", method: http.MethodPost, body: []byte("{\"name\":\"O'Reilly\",\"line\":\"世界\"}\n")},
		{name: "binary", method: http.MethodPut, body: []byte{0, 1, 2, 255, 10}},
		{name: "literal at sign", method: http.MethodPost, body: []byte("@this-is-not-a-file")},
		{name: "binary header", method: http.MethodGet},
		{name: "empty POST", method: http.MethodPost},
		{name: "HEAD", method: http.MethodHead},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requests := make(chan receivedRequest, 2)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
				}

				requests <- receivedRequest{method: r.Method, uri: r.RequestURI, host: r.Host, headers: r.Header.Clone(), body: body}
				_, _ = io.WriteString(w, "ok")
			}))
			defer server.Close()
			log, sink := newLogger(t, logger.LevelDebug)
			client := newClient(t, httpclient.Config{Logger: log})
			marker := filepath.Join(t.TempDir(), "shell-must-not-run")
			req := newRequest(t, tt.method, server.URL+"/a/../orders?filter=[x]&name=O'Reilly", bytes.NewReader(tt.body))
			req.Host = "orders.internal"
			req.Header.Set("Authorization", "Bearer full-secret-for-replay")
			req.Header.Set("Cookie", "session=full-cookie")
			req.Header.Set("X-Quoted", "O'Reilly; $(touch "+marker+") `touch "+marker+"`")
			req.Header.Add("X-Multiple", "first")
			req.Header.Add("X-Multiple", "second")
			req.Header.Set("X-Empty", "")
			if tt.name == "binary header" {
				req.Header.Set("X-Binary", string([]byte{255})+"$(touch "+marker+")")
			}

			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}

			closeResponse(t, resp)
			original := <-requests
			record := sink.wait(t)
			command, ok := record.Fields["curl"].(string)
			if !ok {
				t.Fatalf("curl unavailable: %v", record.Fields)
			}

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			if output, err := exec.CommandContext(ctx, "sh", "-c", command).CombinedOutput(); err != nil {
				t.Fatalf("curl replay: %v: %s", err, output)
			}

			replayed := <-requests
			if !reflect.DeepEqual(original, replayed) {
				t.Errorf("replayed request differs\noriginal: %+v\nreplayed: %+v", original, replayed)
			}

			if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Fatal("a request value was executed by the shell")
			}
		})
	}
}

func TestCurl_SuppressedDefaultHeaders(t *testing.T) {
	requests := make(chan http.Header, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		requests <- r.Header.Clone()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	log, sink := newLogger(t, logger.LevelDebug)
	base := http.DefaultTransport.(*http.Transport).Clone()
	base.DisableCompression = true
	client := newClient(t, httpclient.Config{Logger: log, Transport: base})
	req := newRequest(t, http.MethodPost, server.URL, bytes.NewReader([]byte("body without content type")))
	req.Header["User-Agent"] = nil
	req.Header.Set("Accept", "")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	closeResponse(t, resp)
	original := <-requests
	record := sink.wait(t)
	command, ok := record.Fields["curl"].(string)
	if !ok {
		t.Fatalf("curl unavailable: %v", record.Fields)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if output, err := exec.CommandContext(ctx, "sh", "-c", command).CombinedOutput(); err != nil {
		t.Fatalf("curl replay: %v: %s", err, output)
	}

	if replayed := <-requests; !reflect.DeepEqual(original, replayed) {
		t.Errorf("suppressed headers differ: original=%v replayed=%v", original, replayed)
	}
}

func TestCurl_RedirectLogsEachRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.URL.Path == "/start" {
			http.Redirect(w, r, "/end", http.StatusTemporaryRedirect)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	log, sink := newLogger(t, logger.LevelDebug)
	client := newClient(t, httpclient.Config{Logger: log})

	resp, err := client.Do(newRequest(t, http.MethodPost, server.URL+"/start", bytes.NewReader([]byte("body"))))
	if err != nil {
		t.Fatal(err)
	}

	closeResponse(t, resp)
	sink.wait(t)
	sink.wait(t)
	records := sink.records(t)
	if len(records) != 2 || records[0].Fields["url"] != server.URL+"/start" || records[1].Fields["url"] != server.URL+"/end" {
		t.Errorf("unexpected redirect logs: %+v", records)
	}
}

type receivedRequest struct {
	method  string
	uri     string
	host    string
	headers http.Header
	body    []byte
}
