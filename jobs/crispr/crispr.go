package crispr

import (
	"context"
	"os/exec"
	"path/filepath"

	"github.com/antonybholmes/go-jobs"
	awsjobs "github.com/antonybholmes/go-jobs/aws"
	"github.com/antonybholmes/go-sys"
	awssys "github.com/antonybholmes/go-sys/aws"
	"github.com/antonybholmes/go-sys/log"
)

type (
	CrisprProcessor struct {
		s3Store    *awssys.S3Store
		s3JobStore *awsjobs.S3JobStore
		jobQueue   *awsjobs.SQSJobQueue
		tmpDir     string
	}
)

const (
	CrisprPythonScript = "scripts/jobs/crispr/crispr.py"
)

func NewCrisprProcessor(s3Store *awssys.S3Store,
	s3JobStore *awsjobs.S3JobStore,
	jobQueue *awsjobs.SQSJobQueue,
	tmpDir string) *CrisprProcessor {
	return &CrisprProcessor{
		s3Store:    s3Store,
		s3JobStore: s3JobStore,
		jobQueue:   jobQueue,
		tmpDir:     tmpDir + "/crispr",
	}
}

func (p *CrisprProcessor) ProcessCrisprFile(ctx context.Context, job *jobs.Job) error {
	log.Info().Str("jobId", job.ID).Msg("Received job")

	if len(job.Files) == 0 {
		log.Error().Str("jobId", job.ID).Msg("No files in job")
		return nil
	}

	jobFile := job.Files[0]

	localPath, err := p.s3JobStore.DownloadJobFileLocal(ctx, jobFile, p.tmpDir)

	if err != nil {
		log.Error().Str("jobId", job.ID).Err(err).Msg("Failed to download job file")
		return err
	}

	// process the downloaded local file

	// run shell command or any processing needed on the local file
	log.Info().Str("jobId", job.ID).Str("localPath", localPath).Msg("Processing local file")
	outputPath, err := p.processLocalFile(job, localPath)

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
	key := awsjobs.MakeS3JobFileOutputKey(job, filename)

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
		Status: jobs.JobStatusCompleted,
		Files:  []*jobs.JobFile{outputFile},
	}

	p.jobQueue.SendJob(ctx, &newJob)

	// Clean up local files if needed
	//os.Remove(localPath)
	//os.Remove(outputPath)

	return nil
}

func (p *CrisprProcessor) processLocalFile(job *jobs.Job, path string) (string, error) {
	// Implement the actual processing logic here
	// For now, just log the processing
	log.Info().Str("jobId", job.ID).Str("localPath", path).Msg("Processing local file")

	out := filepath.Join(p.tmpDir, "crispr_"+job.ID+".txt")

	// Run the shell command
	cmd := exec.Command("python", CrisprPythonScript, "--file="+path, "--out="+out)
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
