package handlers

import (
	"context"
	"fmt"

	"github.com/antonybholmes/go-edb-jobs-server/config"
	crisprjob "github.com/antonybholmes/go-edb-jobs-server/jobs/crispr"
	testjob "github.com/antonybholmes/go-edb-jobs-server/jobs/test"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/antonybholmes/go-jobs"
	awsjobs "github.com/antonybholmes/go-jobs/aws"
	awssys "github.com/antonybholmes/go-sys/aws"
	"github.com/antonybholmes/go-sys/log"
)

type (
	JobConsumer struct {
		config     *config.AppConfig
		client     *sqs.Client
		s3Store    *awssys.S3Store
		s3JobStore *awsjobs.S3JobStore
		jobQueue   *awsjobs.SQSJobQueue
		emailQueue jobs.MailQueue
		pool       *pgxpool.Pool
	}
)

func NewJobConsumer(cfg *config.AppConfig,
	pool *pgxpool.Pool,
	sqsClient *sqs.Client,
	s3Store *awssys.S3Store,
	s3JobStore *awsjobs.S3JobStore,
	jobQueue *awsjobs.SQSJobQueue,
	emailQueue jobs.MailQueue,
) *JobConsumer {
	return &JobConsumer{
		config:     cfg,
		pool:       pool,
		client:     sqsClient,
		jobQueue:   jobQueue,
		s3Store:    s3Store,
		s3JobStore: s3JobStore,
		emailQueue: emailQueue,
	}
}

func (c *JobConsumer) ConsumeJobs(ctx context.Context) error {

	sqsConsumer, err := awssys.NewSQSConsumer(c.client,
		c.config.AWS.SQS.QueueURLs.Jobs,
		c.config.AWS.SQS.Threads,
		c.config.AWS.SQS.WaitTimeSecs,
		c.config.AWS.SQS.MaxReceiveCount,
	)

	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create SQS consumer")
	}

	consumer := awsjobs.NewSQSJobConsumer(sqsConsumer)

	dbStore := jobs.NewPostgresStore(c.pool, c.emailQueue)

	consumer.Consume(ctx, func(ctx context.Context, job *jobs.Job) error {
		// Process the job here

		log.Debug().Msgf("Processing job: job ID: %v", job.ID)

		err := dbStore.InsertJob(ctx, job)

		if err != nil {
			log.Error().Err(err).Msg("Failed to update job status in database")
			return err
		}

		// attempt to run job if just submitted
		if job.Status == jobs.JobStatusSubmitted {
			switch job.Type {
			case "crispr":
				processor := crisprjob.NewCrisprProcessor(
					c.s3Store,
					c.s3JobStore,
					c.jobQueue,
					c.config.Files.TmpDir,
				)
				return processor.ProcessCrisprFile(ctx, job)
			case "test":
				processor := testjob.NewTestProcessor(
					c.s3Store,
					c.s3JobStore,
					c.jobQueue,
					c.config.Files.TmpDir,
				)
				return processor.ProcessCrisprFile(ctx, job)
			default:
				log.Error().Msgf("Unknown job type: %s", job.Type)
				return fmt.Errorf("unknown job type: %s", job.Type)
			}
		}

		// switch job.Status {
		// case jobs.JobStatusSubmitted:
		// 	err := dbStore.InsertJob(ctx, job)

		// 	if err != nil {
		// 		log.Error().Err(err).Msg("Failed to update job status in database")
		// 		return err
		// 	}
		// case jobs.JobQueueItemTypeStatus:
		// 	// Create job and send email notification if needed
		// 	err := dbStore.UpdateJobStatus(ctx, job.ID, job.Status)

		// 	if err != nil {
		// 		log.Error().Err(err).Msg("Failed to update job status in database")
		// 		return err
		// 	}

		// default:
		// 	log.Error().Msgf("Unknown job status: %s", job.Status)
		// 	return fmt.Errorf("unknown job status: %s", job.Status)

		// }

		return nil
	})

	return nil
}
