package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/antonybholmes/go-jobs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestAWSAdapters(t *testing.T) {
	var objectBody string
	var message map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/bucket/jobs/id/upload":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
			objectBody = string(body)
			if r.Header.Get("Content-Type") != "text/plain" {
				t.Errorf("unexpected content type %q", r.Header.Get("Content-Type"))
			}
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost:
			if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
				t.Error(err)
			}
			w.Header().Set("Content-Type", "application/x-amz-json-1.0")
			io.WriteString(w, `{"MessageId":"test"}`)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("test", "test", "")),
	}
	objects := &S3Store{client: s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(server.URL)
		o.UsePathStyle = true
	})}
	j := jobs.Job{Bucket: "bucket", Key: "jobs/id/upload", ContentType: "text/plain", Size: 5}
	if err := objects.Upload(context.Background(), j, strings.NewReader("hello")); err != nil {
		t.Fatal(err)
	}
	if objectBody != "hello" {
		t.Fatalf("unexpected object body %q", objectBody)
	}
	// sender := awsweb.NewSQSSender(sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(server.URL) }), server.URL+"/123/jobs.fifo")
	// if err := sender.Send(context.Background(), []byte(`{"id":"job-id"}`)); err != nil {
	// 	t.Fatal(err)
	// }

	// if message["QueueUrl"] != sender.QueueURL || message["MessageBody"] != `{"id":"job-id"}` || message["MessageGroupId"] != "jobs" || message["MessageDeduplicationId"] != "job-id" {
	// 	t.Fatalf("unexpected FIFO message: %#v", message)
	// }
	// sender.QueueURL = server.URL + "/123/jobs"
	// message = nil
	// if err := sender.Send(context.Background(), []byte(`{"id":"job-id"}`)); err != nil {
	// 	t.Fatal(err)
	// }
	if _, ok := message["MessageGroupId"]; ok {
		t.Fatalf("standard queue should not receive FIFO fields: %#v", message)
	}
	if _, ok := message["MessageDeduplicationId"]; ok {
		t.Fatalf("standard queue should not receive FIFO fields: %#v", message)
	}
}
