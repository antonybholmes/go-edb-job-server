package jobs

import (
	"os"
	"path/filepath"

	"github.com/antonybholmes/go-jobs"
	"github.com/antonybholmes/go-sys"
)

func MakeLocalDir(tmpDir string, job *jobs.Job) (string, error) {
	path := filepath.Join(tmpDir, job.User.ID, job.Type, job.ID)

	err := os.MkdirAll(path, sys.FileModeOwnerFullGroupOtherReadEnter)
	if err != nil {
		return "", err
	}

	return path, nil
}

func MakeLocalFilePath(tmpDir string, job *jobs.Job, jobFile *jobs.JobFile) (string, error) {
	dir, err := MakeLocalDir(tmpDir, job)
	if err != nil {
		return "", err
	}

	path := filepath.Join(dir, filepath.Base(jobFile.ObjectKey))

	return path, nil
}
