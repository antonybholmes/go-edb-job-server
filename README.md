# Job submission service

A Go/Gin HTTP service that uploads a file to S3, stores a job in PostgreSQL,
and delivers a job message to SQS using a transactional outbox.

## Setup

Install Go 1.27.1 or newer. Create a PostgreSQL database, an S3 bucket and an SQS queue. The service uses
the AWS SDK's standard credential and region configuration (for example an IAM
role or the shared AWS configuration file). Its identity needs `s3:PutObject`
on the bucket and `sqs:SendMessage` on the queue. The PostgreSQL user needs
permission to create tables and indexes on first startup and to read and write
them afterward. The application initializes the schema in [schema.sql](schema.sql)
on startup; use a migration system for subsequent schema changes.

Set the required environment variables:

```sh
export DATABASE_URL='postgres://user:password@localhost:5432/jobs'
export AWS_REGION='us-east-1'
export S3_BUCKET='my-job-uploads'
export SQS_QUEUE_URL='https://sqs.us-east-1.amazonaws.com/123456789012/jobs'
go run .
```

Optional settings:

| Variable               | Default     | Description                                                                   |
| ---------------------- | ----------- | ----------------------------------------------------------------------------- |
| `ADDR`                 | `:8080`     | HTTP listen address                                                           |
| `MAX_BODY_BYTES`       | `104857600` | Maximum entire HTTP request body, including multipart overhead; at most 5 GiB |
| `OUTBOX_POLL_INTERVAL` | `5s`        | Time between polls when the outbox is empty or delivery fails                 |

## Submit

```sh
curl -X POST -F 'file=@./input.txt' http://localhost:8080/submit
```

The route accepts `multipart/form-data` with a `file` field. On success it
returns HTTP 202 and `{"job_id":"<uuidv7>"}`. A 202 means the S3 upload and
the PostgreSQL transaction have succeeded; SQS delivery is asynchronous. S3
objects use the key `jobs/<job_id>/upload`.

The SQS message body is JSON with these fields: `id` (UUIDv7), `bucket`, `key`,
`filename`, `content_type`, `size` (bytes), and `created_at` (UTC timestamp).
FIFO queues are supported with a shared message group and the job ID as the
deduplication ID.

The job and its outbox entry are written in one transaction. A background
worker retries pending entries while the application is running. Multiple
instances can safely claim different rows, but messages are **at least once**:
if SQS accepts a message and the database update fails, it can be delivered
again. Consumers must deduplicate by `id`. A persistent SQS error for the oldest
message can delay later messages; monitor logs and the `job_outbox` table.

S3 and PostgreSQL cannot share a transaction. If a database write or commit
fails after upload, the request returns an error and the object may be orphaned.
The object is deliberately not deleted because a failed commit can have an
unknown outcome. Reconcile unreferenced S3 objects against the `jobs` table
before deleting them. Similarly, a client timeout or lost response can leave a
successfully submitted job; retrying the request creates a new job.

Do not expose this endpoint publicly without appropriate authentication,
authorization, request timeouts, and upload/content validation.

## Tests

Run `go test ./...` for unit tests. To include the PostgreSQL integration test,
set `TEST_DATABASE_URL` to a test database whose user can create and drop
schemas, then run `go test ./...`. The test creates and removes its own schema.

## Setting up s3 and cloudfront

### Step 1: Prepare Your S3 Bucket

1. Open the S3 Console and select your bucket (or create a new one).
2. Go to the Permissions tab and ensure Block all public access is turned On. (CloudFront does not require a public bucket).
3. Upload your files to the bucket (e.g., index.html or images/logo.png).

### Step 2: Create the CloudFront Distribution

1. Open the CloudFront Console and click Create distribution.
2. Under Origin domain, click the search bar and select your S3 bucket from the dropdown list (do not use an S3 static website endpoint).
3. Under Origin access, select Origin access control settings (recommended).
4. Click Create new OAC, keep the default settings, and click Create.
5. Scroll down to Web Application Firewall (WAF) and choose whether to enable security protections (or select Do not enable security protections to avoid extra WAF charges during testing).
6. (Optional) If you are hosting a website, enter index.html in the Default root object field at the bottom.
