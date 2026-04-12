package httpencoding

import (
	"compress/gzip"
	"io"
	"net/http"

	"github.com/klauspost/compress/zstd"
)

type resetWriteCloser interface {
	io.WriteCloser
	Reset(io.Writer)
}

// Middleware applies HTTP response compression when negotiated via
// Accept-Encoding. It prefers zstd over gzip and preserves Flush behavior
// for streaming handlers when the underlying writer supports http.Flusher.
// The encodings parameter limits which compression encodings the server will
// offer; nil means use DefaultEncodings, a non-nil empty slice disables
// compression entirely.
func Middleware(next http.Handler, encodings []string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		encoding := Preferred(r.Header.Values("Accept-Encoding"), encodings)
		if encoding == "" {
			next.ServeHTTP(w, r)
			return
		}

		cw := newCompressedWriter(w, encoding)
		defer cw.Close()

		next.ServeHTTP(cw.responseWriter(), r)
	})
}

type compressedWriter struct {
	base       http.ResponseWriter
	flusher    http.Flusher
	statusCode int
	headerSent bool
	encoding   string
	writer     resetWriteCloser
}

func newCompressedWriter(base http.ResponseWriter, encoding string) *compressedWriter {
	cw := &compressedWriter{base: base, encoding: encoding}
	if flusher, ok := base.(http.Flusher); ok {
		cw.flusher = flusher
	}
	return cw
}

func (w *compressedWriter) responseWriter() http.ResponseWriter {
	if w.flusher == nil {
		return (*compressedResponseWriter)(w)
	}
	return (*compressedStreamingResponseWriter)(w)
}

func (w *compressedWriter) Header() http.Header {
	return w.base.Header()
}

func (w *compressedWriter) WriteHeader(statusCode int) {
	if w.headerSent {
		return
	}
	w.statusCode = statusCode
}

func (w *compressedWriter) Write(p []byte) (int, error) {
	if err := w.ensureHeaderAndWriter(); err != nil {
		return 0, err
	}
	return w.writer.Write(p)
}

func (w *compressedWriter) ensureHeaderAndWriter() error {
	if w.headerSent {
		return nil
	}

	if w.statusCode == 0 {
		w.statusCode = http.StatusOK
	}

	w.base.Header().Add("Vary", "Accept-Encoding")
	w.base.Header().Set("Content-Encoding", w.encoding)
	w.base.Header().Del("Content-Length")

	w.base.WriteHeader(w.statusCode)
	w.headerSent = true

	switch w.encoding {
	case "gzip":
		w.writer = gzip.NewWriter(w.base)
	default:
		encoder, err := zstd.NewWriter(w.base)
		if err != nil {
			return err
		}
		w.writer = encoder
	}

	return nil
}

func (w *compressedWriter) Close() error {
	if !w.headerSent {
		if w.statusCode == 0 {
			w.statusCode = http.StatusOK
		}
		w.base.WriteHeader(w.statusCode)
		w.headerSent = true
		return nil
	}

	if w.writer != nil {
		return w.writer.Close()
	}
	return nil
}

func (w *compressedWriter) Flush() {
	if w.flusher == nil {
		return
	}
	if err := w.ensureHeaderAndWriter(); err != nil {
		return
	}
	if w.writer == nil {
		w.flusher.Flush()
		return
	}
	if err := w.writer.Close(); err != nil {
		return
	}
	w.flusher.Flush()
	w.writer.Reset(w.base)
}

type compressedResponseWriter compressedWriter

func (w *compressedResponseWriter) Header() http.Header {
	return (*compressedWriter)(w).Header()
}

func (w *compressedResponseWriter) WriteHeader(statusCode int) {
	(*compressedWriter)(w).WriteHeader(statusCode)
}

func (w *compressedResponseWriter) Write(p []byte) (int, error) {
	return (*compressedWriter)(w).Write(p)
}

type compressedStreamingResponseWriter compressedWriter

func (w *compressedStreamingResponseWriter) Header() http.Header {
	return (*compressedWriter)(w).Header()
}

func (w *compressedStreamingResponseWriter) WriteHeader(statusCode int) {
	(*compressedWriter)(w).WriteHeader(statusCode)
}

func (w *compressedStreamingResponseWriter) Write(p []byte) (int, error) {
	return (*compressedWriter)(w).Write(p)
}

func (w *compressedStreamingResponseWriter) Flush() {
	(*compressedWriter)(w).Flush()
}
