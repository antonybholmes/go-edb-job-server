package local

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/antonybholmes/go-jobs"
	sysfiles "github.com/antonybholmes/go-sys/files"
	"github.com/antonybholmes/go-sys/log"

	awsjobs "github.com/antonybholmes/go-jobs/aws"

	awssys "github.com/antonybholmes/go-sys/aws"
)

type (
	JobFunc func(ctx context.Context,
		job *jobs.Job,
		inputFiles []*jobs.JobFile,
		workDir string,
		inputPaths []string) ([]string, error)

	JobProcessor interface {
		ProcessFile(ctx context.Context, job *jobs.Job) error
	}

	LocalFileJobProcessor struct {
		jobQueue   jobs.JobQueue
		s3Store    *awssys.S3Store
		s3JobStore *awsjobs.S3JobStore
		handlers   map[jobs.JobType]JobFunc
		tmpDir     string
	}
)

// Orchestrates getting files from s3, processing them locally, and uploading results back to s3
func NewLocalFileJobProcessor(s3Store *awssys.S3Store,
	s3JobStore *awsjobs.S3JobStore,
	jobQueue jobs.JobQueue,
	tmpDir string,
	handlers map[jobs.JobType]JobFunc) *LocalFileJobProcessor {
	return &LocalFileJobProcessor{
		s3Store:    s3Store,
		s3JobStore: s3JobStore,
		jobQueue:   jobQueue,
		tmpDir:     tmpDir,
		handlers:   handlers,
	}
}

func (p *LocalFileJobProcessor) ProcessFile(ctx context.Context, job *jobs.Job) error {
	log.Info().Str("jobId", job.ID).Msg("Received job")

	if len(job.Files) == 0 {
		log.Error().Str("jobId", job.ID).Msg("No files in job")
		return nil
	}

	// Create a local directory for the job files based on the job id, user and job type
	localDir, err := MakeLocalDir(p.tmpDir, job)
	if err != nil {
		log.Error().Str("jobId", job.ID).Err(err).Msg("Failed to create local directory")
		return err
	}

	//
	// Download input files from S3 to the local directory
	//

	jobInputFiles := job.InputFiles()
	localPaths := make([]string, len(jobInputFiles))

	for i, jobFile := range jobInputFiles {
		localPath := filepath.Join(localDir, filepath.Base(jobFile.ObjectKey))

		_, err = p.s3JobStore.DownloadJobFileLocal(ctx, jobFile, localPath)

		if err != nil {
			log.Error().Str("jobId", job.ID).Err(err).Msg("Failed to download job file")
			return err
		}

		localPaths[i] = localPath
	}

	//
	// Find a job type handler and proces the local files.
	// The processor will create temp files and return the paths
	// to these.

	handler, ok := p.handlers[job.Type]

	if !ok {
		log.Error().Str("jobId", job.ID).Msg("No handler for job type")
		return nil
	}

	outputPaths, err := handler(ctx, job, jobInputFiles, localDir, localPaths)

	if err != nil {
		log.Error().Str("jobId", job.ID).Err(err).Msg("Failed to process local file")
		return err
	}

	// Determine file sizes of each file
	// outputSizes := make([]int64, len(outputPaths))
	// for i, outputPath := range outputPaths {
	// 	stat, err := os.Stat(outputPath)
	// 	if err != nil {
	// 		log.Error().Str("jobId", job.ID).Err(err).Msg("Failed to stat output file")
	// 		return err
	// 	}
	// 	log.Debug().Str("jobId", job.ID).Str("file", outputPath).Int64("size", stat.Size()).Msg("Output file size")
	// 	outputSizes[i] = stat.Size()
	// }

	//
	// Upload the output files to S3 and create new job file entries
	//

	//outputFiles := make([]*jobs.JobFile, len(outputPaths))

	// make zip file name from the word output and the date and time
	zipFilename := fmt.Sprintf("%s_results_%s.zip", job.Type, time.Now().Format("20060102-150405"))

	zipKey := awsjobs.MakeS3JobFileOutputKey(job, zipFilename)

	// Upload the output files as a zip archive to S3
	fileSizeBytes, err := p.s3Store.ZipAndUpload(ctx, zipKey, outputPaths)

	if err != nil {
		log.Error().Str("jobId", job.ID).Err(err).Msg("Failed to upload zip to S3")
		return err
	}

	outputFile := jobs.NewJobFile(jobs.JobFileTypeOutput,
		zipKey,
		zipFilename,
		fileSizeBytes,
		nil)

	//
	// Make a new job with the output files and mark it as completed and
	// send the new job to the job queue so it can be marked as finished
	// and notify the user.
	//

	// clone the job and change its details
	newJob := job.Copy()
	newJob.Status = jobs.JobStatusCompleted
	newJob.Files = []*jobs.JobFile{outputFile}

	log.Debug().Msgf("new job %v", *newJob)

	if err := p.jobQueue.SendJob(ctx, newJob); err != nil {
		log.Error().Str("jobId", job.ID).Err(err).Msg("Failed to send job to queue")
		return err
	}

	// Clean up local files if needed
	// Note that we use a systemd tmp files cron job to purge old temporary files
	// automatically so this isn't necessary to do manually here.
	//os.Remove(localPath)
	//os.Remove(outputPath)

	return nil
}

// Make a local directory for the job's tmp files and return the path
// Path is based on job and user information.
func MakeLocalDir(tmpDir string, job *jobs.Job) (string, error) {
	path := filepath.Join(tmpDir, job.User.ID, string(job.Type), job.ID)

	err := os.MkdirAll(path, sysfiles.FileModeOwnerFullGroupOtherReadEnter)
	if err != nil {
		return "", err
	}

	return path, nil
}

// Convert a job file to a local file path. The directory for this path
// will be automatically created if it doesn't exist.
func MakeLocalFilePath(tmpDir string, job *jobs.Job, jobFile *jobs.JobFile) (string, error) {
	dir, err := MakeLocalDir(tmpDir, job)
	if err != nil {
		return "", err
	}

	path := filepath.Join(dir, filepath.Base(jobFile.ObjectKey))

	return path, nil
}
