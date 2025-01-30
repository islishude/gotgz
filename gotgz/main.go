package main

import (
	"context"
	"flag"
	"io"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/islishude/gotgz"
)

func main() {
	var (
		Dest     string
		FileName string

		Create  bool
		Extract bool
		Timeout time.Duration

		Relative bool
		LogLevel string

		Algorithm         string
		FileSuffix        string
		NoSameOwner       bool
		NoSamePermissions bool
		NoSameTime        bool
		NoOverwrite       bool
		DryRun            bool
		StripComponents   int64
		Excludes          stringsFlag
	)

	flag.StringVar(&Dest, "C", "", "alias to -directory")
	flag.StringVar(&Dest, "directory", "", "change to DIR before performing any operations")
	flag.StringVar(&LogLevel, "v", slog.LevelInfo.String(), "alias to -verbose")
	flag.StringVar(&LogLevel, "verbose", slog.LevelInfo.String(), "the log level")
	flag.StringVar(&FileName, "f", "", "alias to -file")
	flag.StringVar(&FileName, "file", "", "Use archive file")
	flag.BoolVar(&Create, "c", false, "alias to -create")
	flag.BoolVar(&Create, "create", false, "create a new local archive")
	flag.BoolVar(&Extract, "x", false, "alias to -extract")
	flag.BoolVar(&Extract, "extract", false, "extract files from an archive")
	flag.DurationVar(&Timeout, "timeout", 0, "timeout in go time.Duration expression, if the value is less than or equal to 0, it will be ignored")
	flag.BoolVar(&NoSameOwner, "no-same-owner", true, "(x mode only) Do not extract owner and group IDs.")
	flag.BoolVar(&NoSamePermissions, "no-same-permissions", true, "(x mode only) Do not extract full permissions")
	flag.BoolVar(&NoOverwrite, "no-overwrite", false, "(x mode only) Do not overwrite files")
	flag.BoolVar(&NoSameTime, "no-same-time", true, "(x mode only) Do not extract modification time")
	flag.Int64Var(&StripComponents, "strip-components", 0, "(x mode only) strip N leading components from file names on extraction")
	flag.BoolVar(&Relative, "relative", false, "(x mode only) extract files into a relative path")
	flag.StringVar(&Algorithm, "algo", "gzip", "compression algorithm")
	flag.BoolVar(&DryRun, "dry-run", false, "only print the file list")
	flag.Var(&Excludes, "e", "alias to -exclude")
	flag.Var(&Excludes, "exclude", "(c mode only)exclude files from the tarball, the pattern is the same as the filepath.Match")
	flag.StringVar(&FileSuffix, "suffix", "", "(c mode only) suffix for the archive file")
	flag.Parse()

	if FileName == "" {
		faltaln("File name is empty")
	}

	if !Create && !Extract {
		faltaln("No action :)")
	}

	slog.SetLogLoggerLevel(ParseLogLevel(LogLevel))
	start := time.Now()
	defer func() {
		slog.Info("Time cost:", "period", time.Since(start).String())
	}()

	basectx, cancel := func() (context.Context, context.CancelFunc) {
		if Timeout <= 0 {
			return context.WithCancel(context.Background())
		}
		return context.WithTimeout(context.Background(), Timeout)
	}()
	go func() {
		stopSig := make(chan os.Signal, 1)
		signal.Notify(stopSig, syscall.SIGINT, syscall.SIGTERM)
		<-stopSig
		cancel()
	}()

	source, err := url.Parse(FileName)
	if err != nil {
		faltaln(err.Error())
	}

	archiver, err := gotgz.GetCompressionHandlers(Algorithm)
	if err != nil {
		faltaln(err.Error())
	}

	ctFlags := gotgz.CompressFlags{
		DryRun:   DryRun,
		Relative: Relative,
		Archiver: archiver,
		Exclude:  Excludes,
		Logger:   slog.Default(),
	}

	deFlags := gotgz.DecompressFlags{
		NoSamePerm:  NoSamePermissions,
		NoSameOwner: NoSameOwner,
		NoOverwrite: NoOverwrite,
		NoSameTime:  NoSameTime,
		Archiver:    archiver,
		Logger:      slog.Default(),
	}

	if gotgz.IsS3(source) {
		metadata, err := gotgz.ParseMetadata(source.RawQuery)
		if err != nil {
			faltaln(err.Error())
		}

		client, err := gotgz.New(basectx, source.Host)
		if err != nil {
			faltaln(err.Error())
		}
		switch {
		case Create:
			filePath := gotgz.AddTarSuffix(source.Path, FileSuffix)
			if err := client.Upload(basectx, filePath,
				archiver.MediaType(), metadata, ctFlags, flag.Args()...); err != nil {
				faltaln(err.Error())
			}
		case Extract:
			if _, err := client.Download(basectx, source.Path, Dest, deFlags); err != nil {
				faltaln(err.Error())
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
			if filepath.Ext(FileName) != archiver.Extension() {
				slog.Warn("File extension might be not match", "archive", archiver.Name())
			}
			if err := os.MkdirAll(filepath.Dir(FileName), os.ModePerm); err != nil {
				faltaln(err.Error())
			}
			FileName = gotgz.AddTarSuffix(FileName, FileSuffix)
			buf, err = os.Create(FileName)
			if err != nil {
				faltaln(err.Error())
			}
		}
		if err := gotgz.Compress(buf, ctFlags, flag.Args()...); err != nil {
			faltaln(err.Error())
		}
	case Extract:
		var src io.ReadCloser
		if FileName == "-" {
			src = os.Stdin
		} else {
			src, err = os.Open(FileName)
			if err != nil {
				faltaln(err.Error())
			}
		}
		if err := gotgz.Decompress(src, Dest, deFlags); err != nil {
			faltaln(err.Error())
		}
	}
}
