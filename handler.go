package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/antonybholmes/go-edb-job-server/config"

	"github.com/antonybholmes/go-jobs"
	"github.com/antonybholmes/go-sys/log"
	"github.com/antonybholmes/go-web"
	"github.com/antonybholmes/go-web/middleware"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type ObjectStore interface {
	Upload(ctx context.Context, job jobs.Job, body io.ReadSeeker) error
}

type JobStore interface {
	CreateJob(ctx context.Context, job jobs.Job) error
}

type SubmitHandler struct {
	objectStore ObjectStore
	jobStore    JobStore
	Config      *config.AppConfig
	bucket      string
	maxBody     int64
}

func (h *SubmitHandler) Submit(c *gin.Context) {
	user, err := middleware.GetJwtUser(c)

	if err != nil {
		c.Error(err)
		return
	}

	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, h.maxBody)

	file, fileHeader, err := c.Request.FormFile("file")

	// clean up but seems unnecessary if no multipart form was created
	if c.Request.MultipartForm != nil {
		defer func() {
			if err := c.Request.MultipartForm.RemoveAll(); err != nil {
				log.Error().Msgf("remove multipart temporary files: %v", err)
			}
		}()
	}

	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "request body too large"})
		} else {
			web.BadReqResp(c, errors.New("a multipart file field named file is required"))
		}
		return
	}
	defer file.Close()

	id, err := uuid.NewV7()
	if err != nil {
		log.Error().Msgf("generate job ID: %v", err)

		web.InternalErrorResp(c, errors.New("could not create job"))
		return
	}

	contentType := fileHeader.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	if !validMetadata(fileHeader.Filename, 1024) || !validMetadata(contentType, 255) {
		web.BadReqResp(c, errors.New("invalid file metadata"))
		return
	}

	userID := user.Subject

	baseJob := jobs.BaseJob{
		ID:        id.String(),
		Type:      "crispr",
		CreatedAt: time.Now().UTC(),
	}

	j := jobs.Job{
		BaseJob:     baseJob,
		Bucket:      h.bucket,
		Key:         "users/" + userID + "/jobs/crispr/" + id.String() + "/upload",
		Filename:    fileHeader.Filename,
		ContentType: contentType,
		Size:        fileHeader.Size,
	}

	if err := h.objectStore.Upload(c.Request.Context(), j, file); err != nil {
		log.Error().Msgf("upload job file: job_id=%s error=%v", j.ID, err)
		c.JSON(http.StatusBadGateway, gin.H{"error": "file upload failed"})
		return
	}

	if err := h.jobStore.CreateJob(c.Request.Context(), j); err != nil {
		// A failed commit can have an unknown outcome, so deleting the object here
		// could destroy a successfully committed job's input.
		log.Error().Msgf("persist uploaded job; object may need reconciliation: job_id=%s bucket=%s key=%s error=%v", j.ID, j.Bucket, j.Key, err)
		web.InternalErrorResp(c, errors.New("could not persist job"))
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"job_id": j.ID})
}

func validMetadata(value string, maxBytes int) bool {
	return len(value) <= maxBytes && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}
