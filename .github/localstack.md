Use LocalStack to run AWS services locally.

Add localstack envs:

```sh
export AWS_ENDPOINT_URL=http://127.0.0.1:4566
export AWS_ACCESS_KEY_ID=foo
export AWS_SECRET_ACCESS_KEY=bar
export AWS_REGION=us-east-1
```

Create test bucket:

```sh
docker compose exec localstack awslocal s3api create-bucket --bucket test
```
