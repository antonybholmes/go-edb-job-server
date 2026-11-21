package main

import (
	"context"
	"errors"
	"fmt"
	"go-edb-job-server/config"

	"os"
	"sync"
	"time"

	"github.com/antonybholmes/go-jobs"
	awsjobs "github.com/antonybholmes/go-jobs/aws"
	"github.com/antonybholmes/go-sys/log"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
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

	if _, err := pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("initialize schema: %w", err)

	}

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
func runOutbox(ctx context.Context, store OutboxPublisher, sender MessageSender, interval time.Duration) {
	for ctx.Err() == nil {
		sent, err := store.PublishOne(ctx, sender)
		if err != nil && ctx.Err() == nil {
			log.Error().Msgf("publish outbox message: %v", err)
		}
		if sent && err == nil {
			continue
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func init() {
	log.SetAppName(config.AppName)
}

func main() {
	cfg := config.Instance()

	err := run(cfg)

	if err != nil {
		logger.Error("application stopped", "error", err)
		os.Exit(1)
	}

	sqsConsumer, err := awsjobs.NewSQSConsumer(cfg.AWS.SQS.Jobs.Submitted.QueueURL,
		cfg.AWS.SQS.Threads,
		cfg.AWS.SQS.WaitTimeSecs)

	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create email consumer")
	}

	ctx := context.Background()

	var consumer = awsjobs.NewSQSJobConsumer(sqsConsumer)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		consumer.Consume(ctx, func(ctx context.Context, job *jobs.Job) error {
			// Process the job here
			return nil
		})
	}()
	wg.Wait()
}
