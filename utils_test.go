package gotgz

import (
	"compress/gzip"
	"io"
	"reflect"
	"testing"
)

func Test_stripComponents(t *testing.T) {
	type args struct {
		p string
		n int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "strip 0 components",
			args: args{p: "a/b/c/d", n: 0},
			want: "a/b/c/d",
		},
		{
			name: "strip 1 component",
			args: args{p: "a/b/c/d", n: 1},
			want: "b/c/d",
		},
		{
			name: "strip 2 components",
			args: args{p: "a/b/c/d", n: 2},
			want: "c/d",
		},
		{
			name: "strip all components",
			args: args{p: "a/b/c/d", n: 4},
			want: "d",
		},
		{
			name: "strip more than available components",
			args: args{p: "a/b/c/d", n: 5},
			want: "d",
		},
		{
			name: "strip from single component path",
			args: args{p: "a", n: 1},
			want: "a",
		},
		{
			name: "strip 0 components from single component path",
			args: args{p: "a", n: 0},
			want: "a",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stripComponents(tt.args.p, tt.args.n); got != tt.want {
				t.Errorf("stripComponents() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseMetadata(t *testing.T) {
	type args struct {
		raw string
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]string
		wantErr bool
	}{
		{
			name:    "empty metadata",
			args:    args{raw: ""},
			want:    nil,
			wantErr: false,
		},
		{
			name:    "single key-value pair",
			args:    args{raw: "key=value"},
			want:    map[string]string{"key": "value"},
			wantErr: false,
		},
		{
			name:    "multiple key-value pairs",
			args:    args{raw: "key1=value1&key2=value2"},
			want:    map[string]string{"key1": "value1", "key2": "value2"},
			wantErr: false,
		},
		{
			name:    "key with multiple values",
			args:    args{raw: "key=value1&key=value2"},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "invalid query string",
			args:    args{raw: "key=value1&key2"},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseMetadata(tt.args.raw)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseMetadata() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseMetadata() = %v, want %v", got, tt.want)
			}
		})
	}
}

// a simple io.WriteCloser implementation
// type dummyWriter struct {
// 	io.ReadWriter
// }

// func (d *dummyWriter) Close() error {
// 	return nil
// }

func TestGetCompressionHandlers(t *testing.T) {
	type args struct {
		alg string
	}
	tests := []struct {
		name    string
		args    args
		want    ZipWriter
		want1   ZipReader
		want2   string
		wantErr bool
	}{
		{
			name: "gzip default compression",
			args: args{alg: "gzip"},
			want: func(buf io.WriteCloser) (io.WriteCloser, error) {
				return gzip.NewWriterLevel(buf, gzip.DefaultCompression)
			},
			want1:   func(src io.ReadCloser) (io.Reader, error) { return gzip.NewReader(src) },
			want2:   "application/x-gzip",
			wantErr: false,
		},
		{
			name:    "gzip with specific compression level",
			args:    args{alg: "gzip?level=5"},
			want:    func(buf io.WriteCloser) (io.WriteCloser, error) { return gzip.NewWriterLevel(buf, 5) },
			want1:   func(src io.ReadCloser) (io.Reader, error) { return gzip.NewReader(src) },
			want2:   "application/x-gzip",
			wantErr: false,
		},
		{
			name:    "gzip with invalid compression level",
			args:    args{alg: "gzip?level=invalid"},
			want:    nil,
			want1:   nil,
			want2:   "",
			wantErr: true,
		},
		{
			name:    "unsupported compression algorithm",
			args:    args{alg: "unsupported"},
			want:    nil,
			want1:   nil,
			want2:   "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, got2, err := GetCompressionHandlers(tt.args.alg)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetCompressionHandlers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// if !reflect.DeepEqual(got, tt.want) {
			// 	t.Errorf("GetCompressionHandlers() got = %v, want %v", got, tt.want)
			// }
			// if !reflect.DeepEqual(got1, tt.want1) {
			// 	t.Errorf("GetCompressionHandlers() got1 = %v, want %v", got1, tt.want1)
			// }
			if got2 != tt.want2 {
				t.Errorf("GetCompressionHandlers() got2 = %v, want %v", got2, tt.want2)
			}
		})
	}
}
