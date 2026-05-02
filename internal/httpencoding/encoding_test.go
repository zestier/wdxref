package httpencoding

import "testing"

func TestAccepts(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		target string
		want   bool
	}{
		{name: "exact match", values: []string{"gzip, zstd"}, target: "zstd", want: true},
		{name: "wildcard", values: []string{"*"}, target: "gzip", want: true},
		{name: "quality zero disables", values: []string{"zstd;q=0, gzip"}, target: "zstd", want: false},
		{name: "quality positive enables", values: []string{"gzip;q=0.5"}, target: "gzip", want: true},
		{name: "case insensitive", values: []string{"ZsTd"}, target: "zstd", want: true},
		{name: "unknown target", values: []string{"gzip, zstd"}, target: "br", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Accepts(tc.values, tc.target)
			if got != tc.want {
				t.Fatalf("Accepts(%v, %q) = %v, want %v", tc.values, tc.target, got, tc.want)
			}
		})
	}
}

func TestPreferred(t *testing.T) {
	tests := []struct {
		name    string
		values  []string
		allowed []string
		want    string
	}{
		{name: "prefers zstd", values: []string{"gzip, zstd"}, want: "zstd"},
		{name: "falls back gzip", values: []string{"gzip"}, want: "gzip"},
		{name: "zstd disabled", values: []string{"zstd;q=0, gzip"}, want: "gzip"},
		{name: "no supported encoding", values: []string{"br"}, want: ""},
		{name: "wildcard prefers zstd", values: []string{"*"}, want: "zstd"},
		{name: "allowed gzip only", values: []string{"gzip, zstd"}, allowed: []string{"gzip"}, want: "gzip"},
		{name: "allowed zstd only", values: []string{"gzip, zstd"}, allowed: []string{"zstd"}, want: "zstd"},
		{name: "allowed empty disables", values: []string{"gzip, zstd"}, allowed: []string{}, want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Preferred(tc.values, tc.allowed)
			if got != tc.want {
				t.Fatalf("Preferred(%v, %v) = %q, want %q", tc.values, tc.allowed, got, tc.want)
			}
		})
	}
}

func TestParseEncodings(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
		isNil bool
	}{
		{name: "empty returns nil", input: "", want: nil, isNil: true},
		{name: "single gzip", input: "gzip", want: []string{"gzip"}},
		{name: "single zstd", input: "zstd", want: []string{"zstd"}},
		{name: "both", input: "zstd,gzip", want: []string{"zstd", "gzip"}},
		{name: "with spaces", input: " zstd , gzip ", want: []string{"zstd", "gzip"}},
		{name: "unknown ignored", input: "br,gzip", want: []string{"gzip"}},
		{name: "all unknown", input: "br,identity", want: []string{}},
		{name: "case insensitive", input: "ZSTD,GZIP", want: []string{"zstd", "gzip"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ParseEncodings(tc.input)
			if tc.isNil {
				if got != nil {
					t.Fatalf("ParseEncodings(%q) = %v, want nil", tc.input, got)
				}
				return
			}
			if len(got) != len(tc.want) {
				t.Fatalf("ParseEncodings(%q) = %v, want %v", tc.input, got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("ParseEncodings(%q)[%d] = %q, want %q", tc.input, i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestAcceptHeader(t *testing.T) {
	tests := []struct {
		name      string
		encodings []string
		want      string
	}{
		{name: "nil uses defaults", encodings: nil, want: "zstd, gzip"},
		{name: "gzip only", encodings: []string{"gzip"}, want: "gzip"},
		{name: "empty returns empty", encodings: []string{}, want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := AcceptHeader(tc.encodings)
			if got != tc.want {
				t.Fatalf("AcceptHeader(%v) = %q, want %q", tc.encodings, got, tc.want)
			}
		})
	}
}
