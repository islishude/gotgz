package s3

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// ListPrefix returns all objects whose keys start with prefix.
func (s *Store) ListPrefix(ctx context.Context, bucket string, prefix string) ([]ListedObject, error) {
	pager := awss3.NewListObjectsV2Paginator(s.client, &awss3.ListObjectsV2Input{
		Bucket: new(bucket),
		Prefix: new(prefix),
	})

	var out []ListedObject
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			out = append(out, ListedObject{
				Key:  aws.ToString(obj.Key),
				Size: aws.ToInt64(obj.Size),
			})
		}
	}
	return out, nil
}
