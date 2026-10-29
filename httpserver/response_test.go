package httpserver

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestResponse_StateAndInterfaces(t *testing.T) {
	tests := []struct {
		name   string
		write  func(http.ResponseWriter)
		status int
		bytes  int64
	}{
		{"implicit", func(w http.ResponseWriter) { _, _ = w.Write([]byte("abc")) }, 200, 3},
		{"string", func(w http.ResponseWriter) { _, _ = io.WriteString(w, "abc") }, 200, 3},
		{"copy", func(w http.ResponseWriter) { _, _ = io.Copy(w, io.LimitReader(strings.NewReader("abc"), 3)) }, 200, 3},
		{"headers", func(w http.ResponseWriter) { w.WriteHeader(202); w.WriteHeader(500) }, 202, 0},
		{"informational", func(w http.ResponseWriter) { w.WriteHeader(103); w.WriteHeader(204) }, 204, 0},
		{"flush", func(w http.ResponseWriter) { w.(http.Flusher).Flush(); w.WriteHeader(500) }, 200, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := &responseState{}
			w := state.wrap(httptest.NewRecorder())

			tt.write(w)

			if state.status != tt.status || state.written != tt.bytes {
				t.Errorf("response state = %+v", state)
			}
		})
	}
}

func TestResponse_ReadFromCountsBytes(t *testing.T) {
	state := &responseState{}
	w := state.wrap(&readFromWriter{ResponseRecorder: httptest.NewRecorder()})

	n, err := io.Copy(w, io.LimitReader(strings.NewReader("abc"), 3))
	if err != nil {
		t.Fatal(err)
	}

	if n != 3 || state.written != 3 || state.status != http.StatusOK {
		t.Errorf("ReadFrom state = %+v", state)
	}
}

type readFromWriter struct {
	*httptest.ResponseRecorder
}

func (w *readFromWriter) ReadFrom(r io.Reader) (int64, error) {
	return w.Body.ReadFrom(r)
}
