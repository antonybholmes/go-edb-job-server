package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime"

	"github.com/antonybholmes/go-edb-jobs-server/config"
	"github.com/antonybholmes/go-edb-jobs-server/local"
	"github.com/antonybholmes/go-edb-jobs-server/local/crispr"
	"github.com/antonybholmes/go-edb-jobs-server/local/test"
	"github.com/antonybholmes/go-jobs"

	jobforms "github.com/antonybholmes/go-jobs/forms"
	"github.com/antonybholmes/go-web"
	"github.com/antonybholmes/go-web/auth/token"
	"github.com/antonybholmes/go-web/middleware"
	"github.com/gin-gonic/gin"

	awsmail "github.com/antonybholmes/go-mail/aws"

	"os"
	"sync"

	awsjobs "github.com/antonybholmes/go-jobs/aws"
	awssys "github.com/antonybholmes/go-sys/aws"
	"github.com/antonybholmes/go-sys/log"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/bytedance/gopkg/util/logger"
	"github.com/jackc/pgx/v5/pgxpool"
)

func run(cfg *config.AppConfig) error {

	ctx := context.Background()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load AWS configuration: %w", err)
	}

	if awsCfg.Region == "" {
		return errors.New("AWS region is required; set AWS_REGION")
	}

	pool, err := pgxpool.New(ctx, cfg.App.DatabaseURL)
	if err != nil {
		return fmt.Errorf("configure PostgreSQL: %w", err)
	}

	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("connect to PostgreSQL: %w", err)
	}

	// if _, err := pool.Exec(ctx, schema); err != nil {
	// 	return fmt.Errorf("initialize schema: %w", err)

	// }

	//sqsClient := sqs.NewFromConfig(awsCfg)

	//mailer := awsmail.NewSQSEmailQueue(sqsClient, cfg.AWS.SQS.Email.QueueURL)

	// store := &PostgresStore{Config: cfg, pool: pool, EmailSender: mailer}
	// sender := awsjobs.NewSQSSender(sqsClient, cfg.AWS.SQS.Jobs.QueueURL)

	// handler := &SubmitHandler{
	// 	Config:      cfg,
	// 	bucket:      cfg.AWS.Bucket,
	// 	maxBody:     cfg.App.MaxBodyBytes,
	// 	objectStore: &S3Store{client: s3.NewFromConfig(awsCfg)},
	// 	jobStore:    store,
	// }

	// re := access.NewRuleEngine()
	// re.LoadRules("config/access-rules.json")

	//claimsParser := middleware.NewUserJWTParser(middleware.NewJwtClaimsES256Parser(cfg.Auth.Keys.JwtES256PublicKey))
	//jwtUserMiddleWare := middleware.UserJWTMiddleware(claimsParser)
	//rulesMiddleware := middleware.AccessTokenRulesMiddleware(claimsParser, re)

	// router := gin.New()
	// router.SetTrustedProxies(cfg.App.TrustedProxies)

	// //router.Use(gin.Logger(), gin.Recovery())

	// corsConfig := cors.Config{
	// 	AllowOrigins:     cfg.Cors.AllowOrigins,
	// 	AllowMethods:     cfg.Cors.AllowMethods,
	// 	AllowHeaders:     cfg.Cors.AllowHeaders,
	// 	AllowCredentials: cfg.Cors.AllowCredentials,
	// 	MaxAge:           time.Duration(cfg.Cors.MaxAge) * time.Second,
	// }

	// router.Use(cors.New(corsConfig))                // Check CORS first
	// router.Use(middleware.ErrorHandlerMiddleware()) // Handle errors early

	// router.MaxMultipartMemory = cfg.App.MaxBodyBytes // 8 << 20

	// v1 := router.Group("/v1")

	// crispr := v1.Group("/crispr", rulesMiddleware)
	// crispr.POST("/submit", handler.Submit)

	// // The outbox runs alongside HTTP so it can publish accepted jobs to SQS.
	// go runOutbox(ctx, store, sender, time.Duration(cfg.App.PollIntervalSec)*time.Second)

	// logger.Info("server starting", "address", cfg.App.Port)

	// if err := router.Run(cfg.App.Port); err != nil {
	// 	return fmt.Errorf("serve HTTP: %w", err)
	// }
	// The code below will never be reached because router.Run is blocking
	//return nil

	return nil
}

// runOutbox repeatedly delivers pending messages, polling after an empty queue
// or an error until its context is canceled.
// func runOutbox(ctx context.Context, store jobs.OutboxPublisher, sender jobs.MessageSender, interval time.Duration) {
// 	for ctx.Err() == nil {
// 		sent, err := store.PublishOne(ctx, sender)
// 		if err != nil && ctx.Err() == nil {
// 			log.Error().Msgf("publish outbox message: %v", err)
// 		}
// 		if sent && err == nil {
// 			continue
// 		}
// 		timer := time.NewTimer(interval)
// 		select {
// 		case <-ctx.Done():
// 			timer.Stop()
// 			return
// 		case <-timer.C:
// 		}
// 	}
// }

func init() {
	log.SetAppName(config.AppName)
}

func main() {
	appconfig := config.Instance()

	err := run(appconfig)

	if err != nil {
		logger.Error("application stopped", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal().Msgf("load AWS configuration: %v", err)
	}

	sqsClient := sqs.NewFromConfig(awsCfg)

	pool, err := pgxpool.New(ctx, appconfig.App.DatabaseURL)
	if err != nil {
		log.Fatal().Msgf("configure PostgreSQL: %v", err)
	}

	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Fatal().Msgf("connect to PostgreSQL: %v", err)
	}

	// Create the email queue sending emails in addition to handling job submissions
	emailQueue := awsmail.NewSQSEmailQueue(sqsClient,
		appconfig.App.Email.From,
		appconfig.AWS.SQS.QueueURLs.Email)

	s3Client := s3.NewFromConfig(awsCfg)

	// Create the S3 store for job file uploads.
	s3Store := awssys.NewS3Store(s3Client, appconfig.AWS.S3.Buckets.Jobs)

	// For storing job specific data
	s3JobStore := awsjobs.NewS3JobStore(s3Store)

	// For notifying the job processing queue of new submissions.
	jobQueue := awsjobs.NewSQSJobQueue(sqsClient, appconfig.AWS.SQS.QueueURLs.Jobs)

	// Define all the consumers we need for each queue

	handlers := map[jobs.JobType]local.JobFunc{
		jobs.JobTypeCrispr: crispr.ProcessFile,
		jobs.JobTypeTest:   test.ProcessFile,
	}

	var wg sync.WaitGroup

	// Monitor submitted and completed job queues
	// wg.Go(func() {
	// 	submitJobConsumer := handlers.NewSubmittedJobConsumer(appconfig, sqsClient, emailQueue, pool)
	// 	submitJobConsumer.ConsumeJobs(ctx)
	// })

	// Monitor  job status queue
	wg.Go(func() {
		completedJobConsumer := NewJobConsumer(appconfig,
			pool,
			sqsClient,
			s3Store,
			s3JobStore,
			jobQueue,
			emailQueue,
			handlers)
		completedJobConsumer.ConsumeJobs(ctx)
	})

	runGin(s3JobStore, jobQueue, appconfig)

	wg.Wait()
}

// runGin starts the HTTP server for email requests.
func runGin(
	s3JobStore *awsjobs.S3JobStore,
	jobSender *awsjobs.SQSJobQueue,
	appconfig *config.AppConfig) {
	// once threads are working make a simple server to accept email requests
	// through an HTTP endpoint.

	router := gin.New()
	router.SetTrustedProxies(appconfig.App.TrustedProxies)

	//router.Use(cors.New(corsConfig))                       // Check CORS first
	router.Use(middleware.ErrorHandlerMiddleware()) // Handle errors early
	router.Use(middleware.LoggingMiddleware())      // Log relevant requests only
	//outer.Use(otelgin.Middleware("edb-server"))           // Trace valid requests
	//router.Use(sessions.Sessions(cfg.Session.Name, store)) // Load sessions for routes that need them
	router.Use(gin.Recovery())

	claimsParser := middleware.NewUserJWTParser(middleware.NewJwtClaimsES256Parser(appconfig.Auth.Keys.JwtES256PublicKey))

	jwtIsAccessTokenMiddleware := middleware.JWTIsSpecificTokenTypeMiddleware(token.TokenTypeAccess, claimsParser)

	v1Routes := router.Group("/v1")

	v1Routes.GET("/about",
		func(c *gin.Context) {
			c.JSON(http.StatusOK, web.AboutResp{
				Name:      config.AppName,
				Version:   appconfig.App.Version.Version,
				Build:     appconfig.App.Version.Build,
				Updated:   appconfig.App.Version.Updated,
				Copyright: config.Copyright,
			})
		})

	v1Routes.GET("/info",
		func(c *gin.Context) {
			web.MakeDataResp(c, "", web.InfoResp{
				Arch:   runtime.GOARCH,
				IpAddr: c.ClientIP(),
			})
		})

	jobRoutes := v1Routes.Group("/jobs", jwtIsAccessTokenMiddleware)

	testJobRoute := jobRoutes.Group("/test")

	// Handle job submissions for test jobs
	jobHandler := jobforms.NewSubmitJobHandler("test",
		jobSender,
		s3JobStore,
		appconfig.AWS.S3.Buckets.Jobs,
		int64(appconfig.App.MaxBodyBytes),
	)

	testJobRoute.POST("/submit", jobHandler.SubmitJobHandler)

	router.Run(appconfig.App.Port)
}
