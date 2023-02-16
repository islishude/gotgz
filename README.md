# gotgz

Similar to `tar + gzip`, but supports S3

## Compress

```console
$ gotgz -v -c -f s3://your-s3-bucket/path.tgz public src
a public
a public/favicon.ico
a public/next.svg
a public/thirteen.svg
a public/vercel.svg
a src
a src/app
a src/app/globals.css
a src/app/head.js
a src/app/layout.js
a src/app/page.js
a src/app/page.module.css
a src/pages
a src/pages/api
a src/pages/api/hello.js
Time cost: 7.154367ms
```

`-f` also supports a local path.

## Decompress

```console
$ gotgz -v -c -f s3://your-s3-bucket/path.tgz public src
x public
x public/favicon.ico
x public/next.svg
x public/thirteen.svg
x public/vercel.svg
x src
x src/app
x src/app/globals.css
x src/app/head.js
x src/app/layout.js
x src/app/page.js
x src/app/page.module.css
x src/pages
x src/pages/api
x src/pages/api/hello.js
Time cost: 2.987865ms
```

`-f` also supports a local path.
