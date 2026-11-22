package handlers

import (
	"context"

	"github.com/antonybholmes/go-edb-job-server/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/antonybholmes/go-jobs"
	awsjobs "github.com/antonybholmes/go-jobs/aws"
	"github.com/antonybholmes/go-sys/log"
)

type (
	CompletedJobConsumer struct {
		config     *config.AppConfig
		client     *sqs.Client
		emailQueue jobs.MailQueue
		pool       *pgxpool.Pool
	}
)

func NewCompletedJobConsumer(cfg *config.AppConfig,
	client *sqs.Client,
	emailQueue jobs.MailQueue,
	pool *pgxpool.Pool) *CompletedJobConsumer {
	return &CompletedJobConsumer{
		config:     cfg,
		client:     client,
		emailQueue: emailQueue,
		pool:       pool,
	}
}

func (c *CompletedJobConsumer) ConsumeJobs(ctx context.Context) error {

	sqsConsumer, err := awsjobs.NewSQSConsumer(c.client,
		c.config.AWS.SQS.QueueURLs.Jobs.Status,
		c.config.AWS.SQS.Threads,
		c.config.AWS.SQS.WaitTimeSecs,
	)

	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create SQS consumer")
	}

	consumer := awsjobs.NewSQSJobStatusConsumer(sqsConsumer)

	dbStore := jobs.NewPostgresStore(c.pool, c.emailQueue)

	consumer.Consume(ctx, func(ctx context.Context, job *jobs.JobStatusQueueItem) error {
		// Process the job here

		// Create job and send email notification if needed
		err := dbStore.UpdateJobStatus(ctx, job)

		if err != nil {
			log.Error().Err(err).Msg("Failed to update job status in database")
			return err
		}

		return nil
	})

	return nil
}
