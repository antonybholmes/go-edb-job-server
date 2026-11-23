package handlers

import (
	"context"
	"fmt"

	"github.com/antonybholmes/go-edb-jobs-server/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/antonybholmes/go-jobs"
	awsjobs "github.com/antonybholmes/go-jobs/aws"
	awssys "github.com/antonybholmes/go-sys/aws"
	"github.com/antonybholmes/go-sys/log"
)

type (
	JobStatusConsumer struct {
		config     *config.AppConfig
		client     *sqs.Client
		emailQueue jobs.MailQueue
		pool       *pgxpool.Pool
	}
)

func NewJobStatusConsumer(cfg *config.AppConfig,
	client *sqs.Client,
	emailQueue jobs.MailQueue,
	pool *pgxpool.Pool) *JobStatusConsumer {
	return &JobStatusConsumer{
		config:     cfg,
		client:     client,
		emailQueue: emailQueue,
		pool:       pool,
	}
}

func (c *JobStatusConsumer) ConsumeJobs(ctx context.Context) error {

	sqsConsumer, err := awssys.NewSQSConsumer(c.client,
		c.config.AWS.SQS.QueueURLs.Jobs,
		c.config.AWS.SQS.Threads,
		c.config.AWS.SQS.WaitTimeSecs,
	)

	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create SQS consumer")
	}

	consumer := awsjobs.NewSQSJobConsumer(sqsConsumer)

	dbStore := jobs.NewPostgresStore(c.pool, c.emailQueue)

	consumer.Consume(ctx, func(ctx context.Context, qi *jobs.JobQueueItem) error {
		// Process the job here

		log.Debug().Msgf("Processing job queue item: %v, job ID: %v", qi.Type, qi.Job.ID)

		switch qi.Type {
		case jobs.JobQueueItemTypeSubmit:
			err := dbStore.InsertJob(ctx, qi)

			if err != nil {
				log.Error().Err(err).Msg("Failed to update job status in database")
				return err
			}
		case jobs.JobQueueItemTypeStatus:
			// Create job and send email notification if needed
			err := dbStore.UpdateJobStatus(ctx, qi)

			if err != nil {
				log.Error().Err(err).Msg("Failed to update job status in database")
				return err
			}

		default:
			log.Error().Msgf("Unknown job queue item type: %s", qi.Type)
			return fmt.Errorf("unknown job queue item type: %s", qi.Type)

		}

		return nil
	})

	return nil
}
