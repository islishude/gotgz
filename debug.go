package gotgz

import (
	"fmt"
)

func debugf(l string, p ...interface{}) {
	if Debug {
		fmt.Printf(l, p...)
		fmt.Println()
	}
}
