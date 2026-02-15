package locator

import (
	"fmt"
	"net/url"
	"sort"
	"strings"

	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"
)

type Kind string

const (
	KindLocal Kind = "local"
	KindStdio Kind = "stdio"
	KindS3    Kind = "s3"
)

type Ref struct {
	Kind     Kind
	Raw      string
	Path     string
	Bucket   string
	Key      string
	Metadata map[string]string
}

func ParseArchive(v string) (Ref, error) {
	if v == "-" {
		return Ref{Kind: KindStdio, Raw: v}, nil
	}
	if strings.HasPrefix(v, "s3://") {
		return parseS3URI(v)
	}
	if strings.HasPrefix(v, "arn:") {
		return parseS3ARN(v)
	}
	return Ref{Kind: KindLocal, Raw: v, Path: v}, nil
}

func ParseMember(v string) (Ref, error) {
	if strings.HasPrefix(v, "s3://") {
		return parseS3URI(v)
	}
	if strings.HasPrefix(v, "arn:") {
		return parseS3ARN(v)
	}
	return Ref{Kind: KindLocal, Raw: v, Path: v}, nil
}

func parseS3URI(v string) (Ref, error) {
	u, err := url.Parse(v)
	if err != nil {
		return Ref{}, fmt.Errorf("invalid s3 uri %q: %w", v, err)
	}
	if u.Scheme != "s3" {
		return Ref{}, fmt.Errorf("unsupported uri scheme %q", u.Scheme)
	}
	bucket := u.Host
	key := strings.TrimPrefix(u.Path, "/")
	if bucket == "" {
		return Ref{}, fmt.Errorf("s3 uri must include bucket")
	}
	return Ref{Kind: KindS3, Raw: v, Bucket: bucket, Key: key, Metadata: parseQueryMetadata(u.Query())}, nil
}

func parseS3ARN(v string) (Ref, error) {
	a, err := awsarn.Parse(v)
	if err != nil {
		return Ref{}, fmt.Errorf("invalid arn: %w", err)
	}
	if a.Service != "s3" {
		return Ref{}, fmt.Errorf("unsupported arn service %q", a.Service)
	}

	if strings.HasPrefix(a.Resource, "accesspoint/") {
		parts := strings.SplitN(a.Resource, "/object/", 2)
		if len(parts) != 2 || parts[1] == "" {
			return Ref{}, fmt.Errorf("unsupported accesspoint arn, expected /object/<key>")
		}
		bucketARN := fmt.Sprintf("arn:%s:%s:%s:%s:%s", a.Partition, a.Service, a.Region, a.AccountID, parts[0])
		return Ref{Kind: KindS3, Raw: v, Bucket: bucketARN, Key: parts[1]}, nil
	}

	resource := a.Resource
	if after, ok := strings.CutPrefix(resource, ":::"); ok {
		resource = after
	}
	if after, ok := strings.CutPrefix(resource, "bucket/"); ok {
		resource = after
	}
	parts := strings.SplitN(resource, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return Ref{}, fmt.Errorf("unsupported s3 arn, expected object arn with bucket and key")
	}
	return Ref{Kind: KindS3, Raw: v, Bucket: parts[0], Key: parts[1]}, nil
}

func JoinS3Prefix(prefix, name string) string {
	prefix = strings.TrimPrefix(prefix, "/")
	prefix = strings.TrimSuffix(prefix, "/")
	name = strings.TrimPrefix(name, "/")
	if prefix == "" {
		return name
	}
	if name == "" {
		return prefix
	}
	return prefix + "/" + name
}

func parseQueryMetadata(q url.Values) map[string]string {
	if len(q) == 0 {
		return nil
	}
	out := make(map[string]string, len(q))
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		key := strings.TrimSpace(k)
		if key == "" {
			continue
		}
		values := q[k]
		switch len(values) {
		case 0:
			out[key] = ""
		case 1:
			out[key] = values[0]
		default:
			out[key] = strings.Join(values, ",")
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
