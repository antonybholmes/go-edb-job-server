package handlers

import (
	"context"

	"github.com/antonybholmes/go-edb-job-server/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/antonybholmes/go-jobs"
	jobsaws "github.com/antonybholmes/go-jobs/aws"
	"github.com/antonybholmes/go-sys/log"
)

type (
	SubmitJobConsumer struct {
		config *config.AppConfig
	}
)

func NewSubmitJobConsumer(cfg *config.AppConfig) *SubmitJobConsumer {
	return &SubmitJobConsumer{
		config: cfg,
	}
}

func (c *SubmitJobConsumer) ConsumeJobs(ctx context.Context, client *sqs.Client) error {

	sqsConsumer, err := jobsaws.NewSQSConsumer(c.config.AWS.SQS.Jobs.Submitted.QueueURL,
		c.config.AWS.SQS.Threads,
		c.config.AWS.SQS.WaitTimeSecs,
		client)

	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create SQS consumer")
	}

	consumer := jobsaws.NewSQSJobConsumer(sqsConsumer)

	consumer.Consume(ctx, func(ctx context.Context, job *jobs.Job) error {
		// Process the job here
		return nil
	})

	return nil
}
