package httpencoding

import (
	"strconv"
	"strings"
)

// DefaultEncodings is the priority-ordered list of supported compression
// encodings used when no explicit configuration is provided.
var DefaultEncodings = []string{"zstd", "gzip"}

// Preferred picks the best supported encoding from Accept-Encoding values.
// Only encodings in the allowed list are considered. When allowed is nil the
// DefaultEncodings list is used; a non-nil empty slice disables compression.
func Preferred(values []string, allowed []string) string {
	if allowed == nil {
		allowed = DefaultEncodings
	}
	for _, enc := range allowed {
		if Accepts(values, enc) {
			return enc
		}
	}
	return ""
}

// ParseEncodings parses a comma-separated list of encoding names (e.g.
// "zstd,gzip") into a slice. Only recognised compression names are kept.
// Returns nil when s is empty, signalling "use defaults".
func ParseEncodings(s string) []string {
	if s == "" {
		return nil
	}
	result := []string{} // non-nil so callers can distinguish "explicitly empty"
	for _, part := range strings.Split(s, ",") {
		enc := strings.TrimSpace(strings.ToLower(part))
		switch enc {
		case "zstd", "gzip":
			result = append(result, enc)
		}
	}
	return result
}

// AcceptHeader builds an Accept-Encoding header value from an encoding list.
// When encodings is nil the DefaultEncodings are used. Returns "" when the
// list is empty, meaning no Accept-Encoding header should be sent.
func AcceptHeader(encodings []string) string {
	if encodings == nil {
		encodings = DefaultEncodings
	}
	return strings.Join(encodings, ", ")
}

// Accepts reports whether Accept-Encoding values allow a given encoding.
// Wildcard matches are honored and q=0 disables the encoding.
func Accepts(values []string, target string) bool {
	target = strings.ToLower(target)

	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}

			name, params, _ := strings.Cut(part, ";")
			name = strings.ToLower(strings.TrimSpace(name))
			if name != target && name != "*" {
				continue
			}

			quality := 1.0
			for _, param := range strings.Split(params, ";") {
				key, rawValue, ok := strings.Cut(strings.TrimSpace(param), "=")
				if !ok || !strings.EqualFold(key, "q") {
					continue
				}
				parsed, err := strconv.ParseFloat(strings.TrimSpace(rawValue), 64)
				if err != nil {
					quality = 0
				} else {
					quality = parsed
				}
				break
			}

			if quality > 0 {
				return true
			}
		}
	}

	return false
}
