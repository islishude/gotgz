# gotgz

Similar to `tar + gzip`, but supports S3

**differences from tar**

1. gzip is enabled as default, but you can also use zstd or lz4
2. `-xvf` short form is not supported, you should use `-x -f`
3. Only files, directories and symbol links are supported

## Compress

```console
$ gotgz -c -f s3://test/testdata.tar.gz -e 'testdata/.exclude/**' --relative --suffix date testdata
2025/01/30 12:14:41 INFO append target=testdata
2025/01/30 12:14:41 INFO append target=testdata/parent
2025/01/30 12:14:41 INFO append target=testdata/parent/.exclude
2025/01/30 12:14:41 INFO append target=testdata/parent/.exclude/1.txt
2025/01/30 12:14:41 INFO append target=testdata/parent/README
2025/01/30 12:14:41 INFO append target=testdata/parent/README.md
2025/01/30 12:14:41 INFO append target=testdata/parent/css
2025/01/30 12:14:41 INFO append target=testdata/parent/css/index.css
2025/01/30 12:14:41 INFO append target=testdata/parent/favicon-32x32.png
2025/01/30 12:14:41 INFO append target=testdata/parent/index.html
2025/01/30 12:14:41 INFO append target=testdata/parent/index.json
2025/01/30 12:14:41 INFO append target=testdata/parent/javascript
2025/01/30 12:14:41 INFO append target=testdata/parent/js
2025/01/30 12:14:41 INFO append target=testdata/parent/js/index.js
2025/01/30 12:14:41 INFO Time cost: period=26.350083ms
```

`-f` also supports a local path.

You can use `s3://your-s3-bucket/path.tgz?key=value` to add metadata to the object.

The default compression method is gzip.

To use zstd or lz4, you need use `--algo` with options:

```
gotgz -c -algo 'zstd?level=1' -f s3://your-s3-bucket/path.tgz /data
gotgz -c -algo 'lz4?level=1' -f s3://your-s3-bucket/path.tgz /data
```

## Decompress

```console
$ gotgz -x -f s3://your-s3-bucket/path.tgz tmp
2025/01/30 12:25:28 INFO extract target=.
2025/01/30 12:25:28 INFO extract target=parent
2025/01/30 12:25:28 INFO extract target=parent/.exclude
2025/01/30 12:25:28 INFO extract target=parent/.exclude/1.txt
2025/01/30 12:25:28 INFO extract target=parent/README
2025/01/30 12:25:28 INFO extract target=parent/README.md
2025/01/30 12:25:28 INFO extract target=parent/css
2025/01/30 12:25:28 INFO extract target=parent/css/index.css
2025/01/30 12:25:28 INFO extract target=parent/favicon-32x32.png
2025/01/30 12:25:28 INFO extract target=parent/index.html
2025/01/30 12:25:28 INFO extract target=parent/index.json
2025/01/30 12:25:28 INFO extract target=parent/javascript
2025/01/30 12:25:28 INFO extract target=parent/js
2025/01/30 12:25:28 INFO extract target=parent/js/index.js
2025/01/30 12:25:28 INFO Time cost: period=6.547375ms
$ tree tmp
tmp
└── parent
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

5 directories, 7 files
```

`-f` also supports a local path.

Don't forget to add `-algo` if the file is compressed by zstd or lz4.
