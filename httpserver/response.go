package httpserver

import (
	"bufio"
	"io"
	"net"
	"net/http"

	"github.com/felixge/httpsnoop"
)

type responseState struct {
	status    int
	written   int64
	committed bool
	hijacked  net.Conn
	aborted   bool
}

type responseStateKey struct{}

func (s *responseState) commit(code int) {
	if !s.committed {
		s.status = code
		s.committed = true
	}
}

func (s *responseState) wrap(w http.ResponseWriter) http.ResponseWriter {
	return httpsnoop.Wrap(w, httpsnoop.Hooks{
		WriteHeader: func(next httpsnoop.WriteHeaderFunc) httpsnoop.WriteHeaderFunc {
			return func(code int) {
				next(code)
				if code >= 200 || code == http.StatusSwitchingProtocols {
					s.commit(code)
				}
			}
		},
		Write: func(next httpsnoop.WriteFunc) httpsnoop.WriteFunc {
			return func(data []byte) (int, error) {
				s.commit(http.StatusOK)
				n, err := next(data)
				s.written += int64(n)

				return n, err
			}
		},
		WriteString: func(next httpsnoop.WriteStringFunc) httpsnoop.WriteStringFunc {
			return func(data string) (int, error) {
				s.commit(http.StatusOK)
				n, err := next(data)
				s.written += int64(n)

				return n, err
			}
		},
		ReadFrom: func(next httpsnoop.ReadFromFunc) httpsnoop.ReadFromFunc {
			return func(src io.Reader) (int64, error) {
				s.commit(http.StatusOK)
				n, err := next(src)
				s.written += n

				return n, err
			}
		},
		Flush: func(next httpsnoop.FlushFunc) httpsnoop.FlushFunc {
			return func() {
				s.commit(http.StatusOK)
				next()
			}
		},
		FlushError: func(next httpsnoop.FlushErrorFunc) httpsnoop.FlushErrorFunc {
			return func() error {
				s.commit(http.StatusOK)

				return next()
			}
		},
		Hijack: func(next httpsnoop.HijackFunc) httpsnoop.HijackFunc {
			return func() (net.Conn, *bufio.ReadWriter, error) {
				conn, buffer, err := next()
				if err == nil {
					s.hijacked = conn
				}

				return conn, buffer, err
			}
		},
	})
}
