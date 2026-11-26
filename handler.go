package main

import (
	"context"

	"github.com/antonybholmes/go-edb-jobs-server/config"
	"github.com/antonybholmes/go-edb-jobs-server/local"

	dbjobs "github.com/antonybholmes/go-jobs/db"
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
		handlers   map[jobs.JobType]local.JobFunc
	}
)

func NewJobConsumer(cfg *config.AppConfig,
	pool *pgxpool.Pool,
	sqsClient *sqs.Client,
	s3Store *awssys.S3Store,
	s3JobStore *awsjobs.S3JobStore,
	jobQueue *awsjobs.SQSJobQueue,
	emailQueue jobs.MailQueue,
	handlers map[jobs.JobType]local.JobFunc,
) *JobConsumer {
	return &JobConsumer{
		config:     cfg,
		pool:       pool,
		client:     sqsClient,
		jobQueue:   jobQueue,
		s3Store:    s3Store,
		s3JobStore: s3JobStore,
		emailQueue: emailQueue,
		handlers:   handlers,
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

	dbStore := dbjobs.NewPostgresStore(c.pool, c.emailQueue)

	processor := local.NewLocalFileJobProcessor(c.s3Store,
		c.s3JobStore,
		c.jobQueue,
		c.config.Files.TmpDir,
		c.handlers)

	consumer.Consume(ctx, func(ctx context.Context, job *jobs.Job) error {
		// Process the job here

		log.Debug().Msgf("Processing job: job ID: %v, status: %v, type: %v  ", job.ID, job.Status, jobs.JobStatusSubmitted)

		err := dbStore.InsertJob(ctx, job)

		if err != nil {
			log.Error().Err(err).Msg("Failed to update job status in database")
			return err
		}

		// attempt to run job if just submitted
		if job.Status == jobs.JobStatusSubmitted {
			return processor.ProcessFile(ctx, job)
		}

		return nil
	})

	return nil
}
