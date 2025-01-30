package gotgz

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

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

func TestGetCompressionHandlers(t *testing.T) {
	type args struct {
		alg string
	}
	tests := []struct {
		name    string
		args    args
		want    Archiver
		wantErr bool
	}{
		{
			name:    "gzip algorithm",
			args:    args{alg: "gzip"},
			want:    GZipArchiver{Level: -1}, // Assuming GZipArchiver implements Archiver
			wantErr: false,
		},
		{
			name:    "gz algorithm",
			args:    args{alg: "gz?level=1"},
			want:    GZipArchiver{Level: 1}, // Assuming GZipArchiver implements Archiver
			wantErr: false,
		},
		{
			name:    "lz4 algorithm",
			args:    args{alg: "lz4?level=1"},
			want:    Lz4Archiver{Level: 1}, // Assuming Lz4Archiver implements Archiver
			wantErr: false,
		},
		{
			name:    "zstd algorithm",
			args:    args{alg: "zstd?level=1"},
			want:    ZstdArchiver{Level: 1}, // Assuming ZstdArchiver implements Archiver
			wantErr: false,
		},
		{
			name:    "unsupported algorithm",
			args:    args{alg: "unsupported"},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "invalid URL",
			args:    args{alg: "://invalid-url"},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetCompressionHandlers(tt.args.alg)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetCompressionHandlers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetCompressionHandlers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAddFileSuffix(t *testing.T) {
	type args struct {
		fileName string
		suffix   string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Add date suffix",
			args: args{fileName: "example.txt", suffix: "date"},
			want: fmt.Sprintf("example-%s.txt", time.Now().Format("20060102")),
		},
		{
			name: "No suffix",
			args: args{fileName: "example.txt", suffix: ""},
			want: "example.txt",
		},
		{
			name: "Add date suffix to file with path",
			args: args{fileName: "/path/to/example.txt", suffix: "date"},
			want: fmt.Sprintf("/path/to/example-%s.txt", time.Now().Format("20060102")),
		},
		{
			name: "Add date suffix to file with tar extension",
			args: args{fileName: "example.tar.gz", suffix: "date"},
			want: fmt.Sprintf("example-%s.tar.gz", time.Now().Format("20060102")),
		},
		{
			name: "Add date suffix to file with multiple dots",
			args: args{fileName: "example.test.txt", suffix: "date"},
			want: fmt.Sprintf("example.test-%s.txt", time.Now().Format("20060102")),
		},
		{
			name: "Add date suffix to file without extension",
			args: args{fileName: "example", suffix: "date"},
			want: fmt.Sprintf("example-%s", time.Now().Format("20060102")),
		},
		{
			name: "Add date suffix to hidden file",
			args: args{fileName: ".example", suffix: "date"},
			want: ".example",
		},
		{
			name: "Add date suffix to hidden file with extension",
			args: args{fileName: ".example.txt", suffix: "date"},
			want: fmt.Sprintf(".example-%s.txt", time.Now().Format("20060102")),
		},
		{
			name: "Add date suffix to file with multiple extensions",
			args: args{fileName: "example.tar.gz", suffix: "date"},
			want: fmt.Sprintf("example-%s.tar.gz", time.Now().Format("20060102")),
		},
		{
			name: "Add date suffix to file with complex path",
			args: args{fileName: "/complex/path/to/example.tar.gz", suffix: "date"},
			want: fmt.Sprintf("/complex/path/to/example-%s.tar.gz", time.Now().Format("20060102")),
		},
		{
			name: "Add custom suffix",
			args: args{fileName: "example.txt", suffix: "custom"},
			want: "example-custom.txt",
		},
		{
			name: "Add custom suffix to file with path",
			args: args{fileName: "/path/to/example.txt", suffix: "custom"},
			want: "/path/to/example-custom.txt",
		},
		{
			name: "Add custom suffix to file with tar extension",
			args: args{fileName: "example.tar.gz", suffix: "custom"},
			want: "example-custom.tar.gz",
		},
		{
			name: "Add custom suffix to file with multiple dots",
			args: args{fileName: "example.test.txt", suffix: "custom"},
			want: "example.test-custom.txt",
		},
		{
			name: "Add custom suffix to file without extension",
			args: args{fileName: "example", suffix: "custom"},
			want: "example-custom",
		},
		{
			name: "Add custom suffix to hidden file",
			args: args{fileName: ".example", suffix: "custom"},
			want: ".example",
		},
		{
			name: "Add custom suffix to hidden file with extension",
			args: args{fileName: ".example.txt", suffix: "custom"},
			want: ".example-custom.txt",
		},
		{
			name: "Add custom suffix to file with multiple extensions",
			args: args{fileName: "example.tar.gz", suffix: "custom"},
			want: "example-custom.tar.gz",
		},
		{
			name: "Add custom suffix to file with complex path",
			args: args{fileName: "/complex/path/to/example.tar.gz", suffix: "custom"},
			want: "/complex/path/to/example-custom.tar.gz",
		},
		{
			name: "Add empty suffix to file with multiple extensions",
			args: args{fileName: "example.tar.gz", suffix: ""},
			want: "example.tar.gz",
		},
		{
			name: "Add empty suffix to file with complex path",
			args: args{fileName: "/complex/path/to/example.tar.gz", suffix: ""},
			want: "/complex/path/to/example.tar.gz",
		},
		{
			name: "Add empty suffix to hidden file",
			args: args{fileName: ".example", suffix: ""},
			want: ".example",
		},
		{
			name: "Add empty suffix to hidden file with extension",
			args: args{fileName: ".example.txt", suffix: ""},
			want: ".example.txt",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AddTarSuffix(tt.args.fileName, tt.args.suffix); got != tt.want {
				t.Errorf("AddSuffix() = %v, want %v", got, tt.want)
			}
		})
	}
}
