package main

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

func faltaln(args ...any) {
	fmt.Println(args...)
	os.Exit(1)
}

type stringsFlag []string

func (a *stringsFlag) Set(s string) error {
	*a = append(*a, s)
	return nil
}

func (a *stringsFlag) String() string {
	return strings.Join(*a, " ")
}

func ParseLogLevel(name string) slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(name)); err == nil {
		return l
	}
	return slog.LevelInfo
}
