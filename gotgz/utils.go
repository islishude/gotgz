package main

import (
	"fmt"
	"os"
	"strings"
)

func faltaln(l string) {
	fmt.Println(l)
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
