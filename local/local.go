package local

import (
	"context"
	"os"
	"path/filepath"

	"github.com/antonybholmes/go-jobs"
	"github.com/antonybholmes/go-sys"
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
		s3Store    *awssys.S3Store
		s3JobStore *awsjobs.S3JobStore
		jobQueue   jobs.JobQueue
		tmpDir     string
		handlers   map[jobs.JobType]JobFunc
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

	//
	// Upload the output files to S3 and create new job file entries
	//

	outputFiles := make([]*jobs.JobFile, len(outputPaths))

	for _, outputPath := range outputPaths {

		file, stat, err := sys.OpenFile(outputPath)

		if err != nil {
			log.Error().Str("jobId", job.ID).Err(err).Msg("Failed to open local file")
			return err
		}
		defer file.Close()

		// need file size for S3 upload
		fileSize := stat.Size()

		// get basename e.g. output.txt
		filename := filepath.Base(outputPath)

		// Make a key to save file
		key := awsjobs.MakeS3JobFileExactOutputKey(job, filename)

		// Write the file to S3
		err = p.s3Store.Upload(ctx, file, key, awssys.ContentTypeTextPlain)

		if err != nil {
			log.Error().Str("jobId", job.ID).Err(err).Msg("Failed to upload file to S3")
			return err
		}

		// make new output job file
		outputFile := jobs.NewJobFile(jobs.JobFileTypeOutput,
			key,
			filename,
			awssys.ContentTypeTextPlain,
			fileSize,
			nil)
		outputFiles = append(outputFiles, outputFile)
	}

	//
	// Make a new job with the output files and mark it as completed and
	// send the new job to the job queue so it can be marked as finished
	// and notify the user.
	//

	newJob := jobs.Job{
		ID:     job.ID,
		User:   job.User,
		Type:   job.Type,
		Bucket: job.Bucket,
		Status: jobs.JobStatusCompleted,
		Files:  outputFiles,
	}

	log.Debug().Msgf("new job %v", &newJob)

	if err := p.jobQueue.SendJob(ctx, &newJob); err != nil {
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

	err := os.MkdirAll(path, sys.FileModeOwnerFullGroupOtherReadEnter)
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
