package crispr

import (
	"context"
	"os/exec"
	"path/filepath"

	"github.com/antonybholmes/go-jobs"
	"github.com/antonybholmes/go-sys/log"
)

const (
	CrisprPythonScript = "./scripts/jobs/crispr/crispr.py"
)

func ProcessFile(ctx context.Context,
	job *jobs.Job,
	inputFiles []*jobs.JobFile,
	workDir string,
	inputPaths []string) ([]string, error) {
	// process the downloaded local file

	localPath := inputPaths[0]

	log.Info().Str("jobId", job.ID).Str("localPath", inputPaths[0]).Msg("Processing local file")

	outputPath := filepath.Join(workDir, "test.txt")

	// Run the shell command
	cmd := exec.Command("python", CrisprPythonScript, "--file="+localPath, "--out="+outputPath)
	err := cmd.Run()
	if err != nil {
		log.Error().Str("jobId", job.ID).Err(err).Msg("Failed to run shell command")
		return nil, err
	}

	outputFiles := []string{outputPath}

	return outputFiles, nil
}
