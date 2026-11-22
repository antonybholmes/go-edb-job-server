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
	SubmittedJobConsumer struct {
		config     *config.AppConfig
		client     *sqs.Client
		emailQueue jobs.MailQueue
		pool       *pgxpool.Pool
	}
)

func NewSubmittedJobConsumer(cfg *config.AppConfig,
	client *sqs.Client,
	emailQueue jobs.MailQueue,
	pool *pgxpool.Pool) *SubmittedJobConsumer {
	return &SubmittedJobConsumer{
		config:     cfg,
		client:     client,
		emailQueue: emailQueue,
		pool:       pool,
	}
}

func (c *SubmittedJobConsumer) ConsumeJobs(ctx context.Context) error {

	sqsConsumer, err := awsjobs.NewSQSConsumer(c.client,
		c.config.AWS.SQS.QueueURLs.Jobs.Submitted,
		c.config.AWS.SQS.Threads,
		c.config.AWS.SQS.WaitTimeSecs,
	)

	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create SQS consumer")
	}

	consumer := awsjobs.NewSQSJobConsumer(sqsConsumer)

	dbStore := jobs.NewPostgresStore(c.pool, c.emailQueue)

	consumer.Consume(ctx, func(ctx context.Context, job *jobs.Job) error {
		// Process the job here

		// Create job and send email notification if needed
		err := dbStore.CreateJob(ctx, job)

		if err != nil {
			log.Error().Err(err).Msg("Failed to create job in database")
			return err
		}

		return nil
	})

	return nil
}
