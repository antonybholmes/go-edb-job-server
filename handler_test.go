package main

// import (
// 	"bytes"
// 	"context"
// 	"encoding/json"
// 	"errors"
// 	"io"
// 	"mime/multipart"
// 	"net/http"
// 	"net/http/httptest"
// 	"strings"
// 	"testing"

// 	"github.com/antonybholmes/go-jobs"
// 	"github.com/gin-gonic/gin"
// 	"github.com/google/uuid"
// )

// type fakeObjects struct {
// 	err    error
// 	job    jobs.Job
// 	data   []byte
// 	called bool
// }

// func (f *fakeObjects) Upload(_ context.Context, j jobs.Job, body io.ReadSeeker) error {
// 	f.called, f.job = true, j
// 	f.data, _ = io.ReadAll(body)
// 	return f.err
// }

// type fakeJobs struct {
// 	err    error
// 	job    jobs.Job
// 	called bool
// }

// func (f *fakeJobs) CreateJob(_ context.Context, j jobs.Job) error {
// 	f.called, f.job = true, j
// 	return f.err
// }

// func multipartRequest(t *testing.T, filename, data string) *http.Request {
// 	t.Helper()
// 	var body bytes.Buffer
// 	writer := multipart.NewWriter(&body)
// 	part, err := writer.CreateFormFile("file", filename)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	if _, err := io.WriteString(part, data); err != nil {
// 		t.Fatal(err)
// 	}
// 	if err := writer.Close(); err != nil {
// 		t.Fatal(err)
// 	}
// 	req := httptest.NewRequest(http.MethodPost, "/submit", &body)
// 	req.Header.Set("Content-Type", writer.FormDataContentType())
// 	return req
// }

// func TestSubmit(t *testing.T) {
// 	gin.SetMode(gin.TestMode)
// 	for _, tc := range []struct {
// 		name        string
// 		request     func(*testing.T) *http.Request
// 		maxBody     int64
// 		uploadErr   error
// 		dbErr       error
// 		wantStatus  int
// 		wantUpload  bool
// 		wantPersist bool
// 	}{
// 		{"success", func(t *testing.T) *http.Request { return multipartRequest(t, "input.txt", "hello") }, 1024, nil, nil, 202, true, true},
// 		{"missing file", func(t *testing.T) *http.Request { return httptest.NewRequest("POST", "/submit", nil) }, 1024, nil, nil, 400, false, false},
// 		{"too large", func(t *testing.T) *http.Request { return multipartRequest(t, "input.txt", strings.Repeat("x", 1024)) }, 256, nil, nil, 413, false, false},
// 		{"invalid metadata", func(t *testing.T) *http.Request { return multipartRequest(t, strings.Repeat("a", 1025), "hello") }, 4096, nil, nil, 400, false, false},
// 		{"upload fails", func(t *testing.T) *http.Request { return multipartRequest(t, "input.txt", "hello") }, 1024, errors.New("S3 unavailable"), nil, 502, true, false},
// 		{"database fails", func(t *testing.T) *http.Request { return multipartRequest(t, "input.txt", "hello") }, 1024, nil, errors.New("database unavailable"), 500, true, true},
// 	} {
// 		t.Run(tc.name, func(t *testing.T) {
// 			objects, jobs := &fakeObjects{err: tc.uploadErr}, &fakeJobs{err: tc.dbErr}
// 			h := &SubmitHandler{bucket: "test-bucket", maxBody: tc.maxBody, objectStore: objects, jobStore: jobs}
// 			router := gin.New()
// 			router.POST("/submit", h.Submit)
// 			response := httptest.NewRecorder()
// 			router.ServeHTTP(response, tc.request(t))
// 			if response.Code != tc.wantStatus || objects.called != tc.wantUpload || jobs.called != tc.wantPersist {
// 				t.Fatalf("status=%d upload=%v persist=%v", response.Code, objects.called, jobs.called)
// 			}
// 			if tc.name != "success" {
// 				return
// 			}
// 			var body struct {
// 				JobID string `json:"job_id"`
// 			}
// 			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
// 				t.Fatal(err)
// 			}
// 			id, err := uuid.Parse(body.JobID)
// 			if err != nil || id.Version() != 7 {
// 				t.Fatalf("expected UUIDv7, got %q", body.JobID)
// 			}
// 			if objects.job != jobs.job || jobs.job.ID != body.JobID || jobs.job.Key != "jobs/"+body.JobID+"/upload" || jobs.job.Filename != "input.txt" || jobs.job.Size != 5 || string(objects.data) != "hello" {
// 				t.Fatalf("unexpected job or data: %+v %q", jobs.job, objects.data)
// 			}
// 		})
// 	}
// }
