package gotgz

import (
	"reflect"
	"testing"
)

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
