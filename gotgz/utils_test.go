package main

import (
	"log/slog"
	"reflect"
	"testing"
)

func TestParseLogLevel(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name string
		args args
		want slog.Level
	}{
		{
			name: "Valid log level debug",
			args: args{name: "debug"},
			want: slog.LevelDebug,
		},
		{
			name: "Valid log level info",
			args: args{name: "info"},
			want: slog.LevelInfo,
		},
		{
			name: "Valid log level warn",
			args: args{name: "warn"},
			want: slog.LevelWarn,
		},
		{
			name: "Valid log level error",
			args: args{name: "error"},
			want: slog.LevelError,
		},
		{
			name: "Invalid log level",
			args: args{name: "invalid"},
			want: slog.LevelInfo,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ParseLogLevel(tt.args.name); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseLogLevel() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStringsFlag_Set(t *testing.T) {
	tests := []struct {
		name    string
		initial stringsFlag
		input   string
		want    stringsFlag
	}{
		{
			name:    "Add single string",
			initial: stringsFlag{},
			input:   "test",
			want:    stringsFlag{"test"},
		},
		{
			name:    "Add multiple strings",
			initial: stringsFlag{"first"},
			input:   "second",
			want:    stringsFlag{"first", "second"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := tt.initial
			if err := a.Set(tt.input); err != nil {
				t.Errorf("Set() error = %v", err)
			}
			if !reflect.DeepEqual(a, tt.want) {
				t.Errorf("Set() = %v, want %v", a, tt.want)
			}
		})
	}
}

func TestStringsFlag_String(t *testing.T) {
	tests := []struct {
		name string
		a    stringsFlag
		want string
	}{
		{
			name: "Single string",
			a:    stringsFlag{"test"},
			want: "test",
		},
		{
			name: "Multiple strings",
			a:    stringsFlag{"first", "second"},
			want: "first second",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}
