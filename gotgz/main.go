package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/islishude/gotgz"
)

func main() {
	var (
		destPath string
		FileName string
		Create   bool
		Extract  bool
		Timeout  time.Duration
	)

	flag.StringVar(&destPath, "C", "", "change to DIR before performing any operations")
	flag.StringVar(&destPath, "directory", "", "change to DIR before performing any operations")
	flag.BoolVar(&gotgz.Debug, "v", true, "debug mode")
	flag.BoolVar(&gotgz.Debug, "verbose", true, "debug mode")
	flag.StringVar(&FileName, "f", "", "Use archive file or device ARCHIVE.")
	flag.StringVar(&FileName, "file", "", "Use archive file or device ARCHIVE.")
	flag.BoolVar(&Create, "c", false, "create a new local archive")
	flag.BoolVar(&Create, "create", false, "create a new local archive")
	flag.BoolVar(&Extract, "x", false, "extract files from an archive")
	flag.BoolVar(&Extract, "extract", false, "extract files from an archive")
	flag.DurationVar(&Timeout, "timeout", time.Hour, "timeout in go time.Duration expression")
	flag.Parse()

	if FileName == "" {
		Faltalf("File name is empty")
	}

	if !Create && !Extract {
		Faltalf("No action :)")
	}

	start := time.Now()
	defer func() {
		if gotgz.Debug {
			fmt.Println("Time cost:", time.Now().Sub(start).String())
		}
	}()

	basectx, cancel := context.WithTimeout(context.Background(), Timeout)
	go func() {
		stopSig := make(chan os.Signal, 1)
		signal.Notify(stopSig, syscall.SIGINT, syscall.SIGTERM)
		<-stopSig
		cancel()
	}()

	fnParsed, err := url.Parse(FileName)
	if err != nil {
		Faltalf(err.Error())
	}

	if IsS3(fnParsed) {
		client := gotgz.New(fnParsed.Host)
		switch {
		case Create:
			if err := client.Upload(basectx, fnParsed.Path, nil, flag.Args()...); err != nil {
				Faltalf(err.Error())
			}
		case Extract:
			if _, err := client.Download(basectx, fnParsed.Path, destPath); err != nil {
				Faltalf(err.Error())
			}
		}
		return
	}

	switch {
	case Create:
		var buf io.WriteCloser
		if FileName == "-" {
			buf = os.Stdout
		} else {
			buf, err = os.OpenFile(FileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				Faltalf(err.Error())
			}
		}
		if err := gotgz.Compress(buf, flag.Args()...); err != nil {
			Faltalf(err.Error())
		}
	case Extract:
		file, err := os.Open(FileName)
		if err != nil {
			Faltalf(err.Error())
		}
		if err := gotgz.Decompress(file, destPath); err != nil {
			Faltalf(err.Error())
		}
	}
}
