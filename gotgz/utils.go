package main

import (
	"fmt"
	"net/url"
	"os"
)

func Faltalf(l string, p ...interface{}) {
	fmt.Printf(l, p...)
	fmt.Println()
	os.Exit(1)
}

func IsS3(u *url.URL) bool {
	return u.Scheme == "s3"
}
