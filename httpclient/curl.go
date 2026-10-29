package httpclient

import (
	"bytes"
	"encoding/base64"
	"maps"
	"net/http"
	"slices"
	"strings"
	"unicode/utf8"
)

func curlCommand(req *http.Request, body []byte, protocol int, disableCompression bool) string {
	method := req.Method
	if method == "" {
		method = http.MethodGet
	}

	args := []string{"curl", "--disable", "--globoff", "--path-as-is", "--proto", shellQuote("=http,https"), "--request", shellQuote(method), "--url", shellQuote(req.URL.String())}
	if method == http.MethodHead {
		args = append(args, "--head")
	}

	headers := req.Header.Clone()
	if headers == nil {
		headers = make(http.Header)
	}

	if req.Host != "" {
		headers.Set("Host", req.Host)
	}

	headers.Del("Content-Length")
	if (req.Body == nil || req.Body == http.NoBody) && (method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch) {
		headers.Set("Content-Length", "0")
	}

	if _, exists := headers["User-Agent"]; !exists {
		agent := "Go-http-client/1.1"
		if protocol == 2 {
			agent = "Go-http-client/2.0"
		}

		headers.Set("User-Agent", agent)
	} else if len(headers["User-Agent"]) == 0 {
		args = append(args, "--header", shellQuote("User-Agent:"))
	}

	if !disableCompression && headers.Get("Accept-Encoding") == "" && headers.Get("Range") == "" && req.Method != http.MethodHead {
		headers.Set("Accept-Encoding", "gzip")
		args = append(args, "--compressed")
	}

	// Suppress curl defaults when the Go request did not supply these headers.
	for _, key := range []string{"Accept", "Content-Type"} {
		if len(headers[key]) == 0 {
			args = append(args, "--header", shellQuote(key+":"))
		}
	}

	for _, key := range slices.Sorted(maps.Keys(headers)) {
		for _, value := range headers[key] {
			header := key + ": " + value
			if value == "" {
				header = key + ";"
				if key == "User-Agent" {
					header = key + ":"
				}
			}

			args = append(args, "--header", shellQuote(header))
		}
	}

	if req.Body != nil && req.Body != http.NoBody {
		if bytes.IndexByte(body, 0) >= 0 || !utf8.Valid(body) || (len(body) != 0 && body[0] == '@') {
			args = append(args, "--data-binary", "@-")
			return "printf '%s' " + shellQuote(base64.StdEncoding.EncodeToString(body)) + " | base64 -d | " + strings.Join(args, " ")
		}

		args = append(args, "--data-binary", shellQuote(string(body)))
	}

	return strings.Join(args, " ")
}

func shellQuote(value string) string {
	if !utf8.ValidString(value) {
		encoded := base64.StdEncoding.EncodeToString([]byte(value))
		return "\"$(printf '%s' '" + encoded + "' | base64 -d)\""
	}

	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}
