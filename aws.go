package main

import (
	"context"
	"fmt"
	"io"

	"github.com/antonybholmes/go-jobs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type (
	S3Store struct {
		client *s3.Client
	}
)

func (s *S3Store) Upload(ctx context.Context, j jobs.Job, body io.ReadSeeker) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(j.Bucket),
		Key:           aws.String(j.Key),
		Body:          body,
		ContentLength: aws.Int64(j.Size),
		ContentType:   aws.String(j.ContentType),
	})
	if err != nil {
		return fmt.Errorf("put S3 object: %w", err)
	}
	return nil
}
