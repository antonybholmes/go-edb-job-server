package test

import (
	"context"
	"os/exec"
	"path/filepath"

	jobfiles "github.com/antonybholmes/go-edb-jobs-server/jobs"
	"github.com/antonybholmes/go-jobs"
	awsjobs "github.com/antonybholmes/go-jobs/aws"
	"github.com/antonybholmes/go-sys"
	awssys "github.com/antonybholmes/go-sys/aws"
	"github.com/antonybholmes/go-sys/log"
)

type (
	TestProcessor struct {
		s3Store    *awssys.S3Store
		s3JobStore *awsjobs.S3JobStore
		jobQueue   *awsjobs.SQSJobQueue
		tmpDir     string
	}
)

const (
	TestPythonScript = "scripts/jobs/test/test.py"
)

func NewTestProcessor(s3Store *awssys.S3Store,
	s3JobStore *awsjobs.S3JobStore,
	jobQueue *awsjobs.SQSJobQueue,
	tmpDir string) *TestProcessor {
	return &TestProcessor{
		s3Store:    s3Store,
		s3JobStore: s3JobStore,
		jobQueue:   jobQueue,
		tmpDir:     tmpDir,
	}
}

func (p *TestProcessor) ProcessCrisprFile(ctx context.Context, job *jobs.Job) error {
	log.Info().Str("jobId", job.ID).Msg("Received job")

	if len(job.Files) == 0 {
		log.Error().Str("jobId", job.ID).Msg("No files in job")
		return nil
	}

	localDir, err := jobfiles.MakeLocalDir(p.tmpDir, job)
	if err != nil {
		log.Error().Str("jobId", job.ID).Err(err).Msg("Failed to create local directory")
		return err
	}

	jobFile := job.Files[0]

	localPath := filepath.Join(localDir, filepath.Base(jobFile.ObjectKey))

	_, err = p.s3JobStore.DownloadJobFileLocal(ctx, jobFile, localPath)

	if err != nil {
		log.Error().Str("jobId", job.ID).Err(err).Msg("Failed to download job file")
		return err
	}

	// process the downloaded local file

	// run shell command or any processing needed on the local file
	log.Info().Str("jobId", job.ID).Str("localPath", localPath).Msg("Processing local file")
	outputPath, err := p.processLocalFile(job, localDir, localPath)

	if err != nil {
		log.Error().Str("jobId", job.ID).Err(err).Msg("Failed to process local file")
		return err
	}

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
		jobFile.Bucket,
		key,
		filename,
		awssys.ContentTypeTextPlain,
		fileSize,
		nil)

	newJob := jobs.Job{
		ID:     job.ID,
		User:   job.User,
		Type:   job.Type,
		Status: jobs.JobStatusCompleted,
		Files:  []*jobs.JobFile{outputFile},
	}

	log.Debug().Msgf("new job %v", &newJob)

	if err := p.jobQueue.SendJob(ctx, &newJob); err != nil {
		log.Error().Str("jobId", job.ID).Err(err).Msg("Failed to send job to queue")
		return err
	}

	// Clean up local files if needed
	//os.Remove(localPath)
	//os.Remove(outputPath)

	return nil
}

func (p *TestProcessor) processLocalFile(job *jobs.Job, localDir string, path string) (string, error) {
	// Implement the actual processing logic here
	// For now, just log the processing
	log.Info().Str("jobId", job.ID).Str("localPath", path).Msg("Processing local file")

	out := filepath.Join(localDir, "test.txt")

	// Run the shell command
	cmd := exec.Command("python", TestPythonScript, "--file="+path, "--out="+out)
	err := cmd.Run()
	if err != nil {
		log.Error().Str("jobId", job.ID).Err(err).Msg("Failed to run shell command")
		return "", err
	}

	return out, nil
}

// func writeLocalFile(file *jobs.JobFile, rc io.ReadCloser) (string, error) {
// 	// key file name is unique so can strip it
// 	filename := filepath.Base(file.Key)

// 	path := "/data/tmp/" + filename

// 	localFile, err := os.Create(path)
// 	if err != nil {
// 		return "", err
// 	}
// 	defer localFile.Close()

// 	_, err = io.Copy(localFile, rc)
// 	if err != nil {
// 		return "", err
// 	}

// 	return path, nil
// }
