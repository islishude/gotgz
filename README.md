# gotgz

Similar to `tar + gzip`, but supports S3

**differences from tar**

1. gzip is enabled by default, but you can also use zstd or lz4
2. `-xvf` short form is not supported, you should use `-x -v -f`
3. Only regular files, directories and symbol links are supported

## Compress

```console
$ gotgz -c -f s3://test/testdata.tar.gz -e 'parent/.exclude/**' -relative -suffix date testdata
2025/01/30 19:19:36 INFO append target=testdata
2025/01/30 19:19:36 INFO append target=testdata/parent
2025/01/30 19:19:36 INFO append target=testdata/parent/README
2025/01/30 19:19:36 INFO append target=testdata/parent/README.md
2025/01/30 19:19:36 INFO append target=testdata/parent/css
2025/01/30 19:19:36 INFO append target=testdata/parent/css/index.css
2025/01/30 19:19:36 INFO append target=testdata/parent/favicon-32x32.png
2025/01/30 19:19:36 INFO append target=testdata/parent/index.html
2025/01/30 19:19:36 INFO append target=testdata/parent/index.json
2025/01/30 19:19:36 INFO append target=testdata/parent/javascript
2025/01/30 19:19:36 INFO append target=testdata/parent/js
2025/01/30 19:19:36 INFO append target=testdata/parent/js/index.js
2025/01/30 19:19:36 INFO Time cost: period=15.129041ms
$ aws s3 ls s3://test
2025-01-30 11:19:36       2332 testdata-20250130.tar.gz
```

`-c` is used to compress files.

`-f` is used to specify the target file, it supports local path and S3 path.

`-e` is used to exclude files or directories, it's a shell glob pattern.

`-relative` is used to keep the relative path in tar ball, if the source directory is `/data` and the file path is `/data/file.txt`, the relative path in tar ball is `file.txt`.

`-suffix` option is used to add a suffix to the file name, date is a built-in suffix.

the last argument is the source directory, it supports multiple directories.

You can use `s3://your-s3-bucket/path.tgz?key=value` to add metadata to the object.

The default compression method is gzip.

To use zstd or lz4, you need use `--algo` with options:

```
gotgz -c -algo 'zstd?level=1' -f s3://your-s3-bucket/path.tgz /data
gotgz -c -algo 'lz4?level=1' -f s3://your-s3-bucket/path.tgz /data
```

## Decompress

```console
$ gotgz -x -f s3://test/testdata.tar.gz -suffix=date -strip-components=1 tmp
2025/01/30 19:21:09 INFO extract target=.
2025/01/30 19:21:09 INFO extract target=parent
2025/01/30 19:21:09 INFO extract target=parent/README
2025/01/30 19:21:09 INFO extract target=parent/README.md
2025/01/30 19:21:09 INFO extract target=parent/css
2025/01/30 19:21:09 INFO extract target=parent/css/index.css
2025/01/30 19:21:09 INFO extract target=parent/favicon-32x32.png
2025/01/30 19:21:09 INFO extract target=parent/index.html
2025/01/30 19:21:09 INFO extract target=parent/index.json
2025/01/30 19:21:09 INFO extract target=parent/javascript
2025/01/30 19:21:09 INFO extract target=parent/js
2025/01/30 19:21:09 INFO extract target=parent/js/index.js
2025/01/30 19:21:09 INFO Time cost: period=4.628542ms
$ tree tmp
tmp
├── README -> README.md
├── README.md
├── css
│   └── index.css
├── favicon-32x32.png
├── index.html
├── index.json
├── javascript -> js
└── js
    └── index.js

4 directories, 7 files
```

`-f` also supports a local path.

The `-strip-components=N` to remove the leading N directories from the file names.

By default, the gotgz will overwrite the existing files, you can use `-no-overwrite=true` to prevent it.

If you want to keep the file permission and user infomation, you can use `-no-same-permissions=false -no-same-owner=false`.

Don't forget to add `-algo` if the file is compressed by zstd or lz4.
