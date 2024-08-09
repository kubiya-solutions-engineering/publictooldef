package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	_ "github.com/mattn/go-sqlite3"
	"github.com/slack-go/slack"
	"golang.org/x/sync/semaphore"
)

type RestoreRequest struct {
	RequestID      string   `json:"request_id"`
	BucketPaths    []string `json:"bucket_paths"`
	ProcessedPaths []string `json:"processed_paths"`
	CreatedAt      string   `json:"created_at"`
	UpdatedAt      string   `json:"updated_at"`
}

var (
	messageTimestamp string
	credsProvider    *customCredentialsProvider
)

type customCredentialsProvider struct {
	creds *aws.Credentials
	mu    sync.RWMutex
}

func (p *customCredentialsProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return *p.creds, nil
}

func (p *customCredentialsProvider) UpdateCredentials(newCreds aws.Credentials) {
	p.mu.Lock()
	defer p.mu.Unlock()
	*p.creds = newCreds
}

func generateRequestID() string {
	bytes := make([]byte, 16)
	_, err := rand.Read(bytes)
	if err != nil {
		log.Fatalf("Failed to generate request ID: %v", err)
	}
	return hex.EncodeToString(bytes)
}

func sendSlackNotification(channel, threadTS string, blocks []slack.Block) error {
	slackToken := os.Getenv("SLACK_API_TOKEN")
	if slackToken == "" {
		log.Println("No SLACK_API_TOKEN set. Slack messages will not be sent.")
		return fmt.Errorf("SLACK_API_TOKEN is not set")
	}

	api := slack.New(slackToken)
	opts := []slack.MsgOption{
		slack.MsgOptionBlocks(blocks...),
	}

	if threadTS != "" {
		opts = append(opts, slack.MsgOptionTS(threadTS))
	}

	if messageTimestamp != "" {
		opts = append(opts, slack.MsgOptionUpdate(messageTimestamp))
	}

	_, newTimestamp, err := api.PostMessage(channel, opts...)
	if err != nil {
		log.Printf("Failed to send Slack message: %v\n", err)
		return err
	}

	if messageTimestamp == "" {
		messageTimestamp = newTimestamp
	}

	return nil
}

func createDBAndRecord(requestID string, bucketPaths []string) error {
	db, err := sql.Open("sqlite3", "./s3_restore_requests.db")
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	createTableQuery := `
	CREATE TABLE IF NOT EXISTS restore_requests (
		request_id TEXT PRIMARY KEY,
		bucket_paths TEXT,
		processed_paths TEXT,
		created_at TEXT,
		updated_at TEXT
	)`
	_, err = db.Exec(createTableQuery)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	bucketPathsJSON, _ := json.Marshal(bucketPaths)
	processedPathsJSON, _ := json.Marshal([]string{})
	insertQuery := `
	INSERT INTO restore_requests (request_id, bucket_paths, processed_paths, created_at, updated_at)
	VALUES (?, ?, ?, ?, ?)`
	_, err = db.Exec(insertQuery, requestID, bucketPathsJSON, processedPathsJSON, time.Now().UTC().Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("failed to insert record: %w", err)
	}

	blocks := []slack.Block{
		slack.NewHeaderBlock(&slack.TextBlockObject{
			Type: slack.PlainTextType,
			Text: ":memo: Created database record",
		}),
		slack.NewSectionBlock(
			&slack.TextBlockObject{
				Type: slack.MarkdownType,
				Text: fmt.Sprintf("*Request ID:* `%s`\n*Created At:* `%s`\n*Updated At:* `%s`\n",
					requestID, time.Now().UTC().Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339)),
			},
			nil,
			nil,
		),
		slack.NewDividerBlock(),
		slack.NewSectionBlock(
			&slack.TextBlockObject{
				Type: slack.MarkdownType,
				Text: "*Bucket Paths:*",
			},
			nil,
			nil,
		),
	}
	for _, path := range bucketPaths {
		blocks = append(blocks, slack.NewSectionBlock(
			&slack.TextBlockObject{
				Type: slack.MarkdownType,
				Text: fmt.Sprintf("- `%s`", path),
			},
			nil,
			nil,
		))
	}

	if err := sendSlackNotification(os.Getenv("SLACK_CHANNEL_ID"), os.Getenv("SLACK_THREAD_TS"), blocks); err != nil {
		log.Printf("Error sending Slack notification: %v\n", err)
	}

	log.Println("Created database record:", requestID)
	return nil
}

func updateProcessedPaths(requestID, processedPath string) error {
	db, err := sql.Open("sqlite3", "./s3_restore_requests.db")
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	var bucketPaths, processedPaths string

	selectQuery := "SELECT bucket_paths, processed_paths FROM restore_requests WHERE request_id = ?"
	err = db.QueryRow(selectQuery, requestID).Scan(&bucketPaths, &processedPaths)
	if err != nil {
		return fmt.Errorf("failed to select paths: %w", err)
	}

	var bp, pp []string
	json.Unmarshal([]byte(bucketPaths), &bp)
	json.Unmarshal([]byte(processedPaths), &pp)

	pp = append(pp, processedPath)
	for i, path := range bp {
		if path == processedPath {
			bp = append(bp[:i], bp[i+1:]...)
			break
		}
	}

	bucketPathsJSON, _ := json.Marshal(bp)
	processedPathsJSON, _ := json.Marshal(pp)
	updateQuery := `
	UPDATE restore_requests
	SET bucket_paths = ?, processed_paths = ?, updated_at = ?
	WHERE request_id = ?`
	_, err = db.Exec(updateQuery, bucketPathsJSON, processedPathsJSON, time.Now().UTC().Format(time.RFC3339), requestID)
	if err != nil {
		return fmt.Errorf("failed to update paths: %w", err)
	}

	blocks := []slack.Block{
		slack.NewHeaderBlock(&slack.TextBlockObject{
			Type: slack.PlainTextType,
			Text: ":hourglass_flowing_sand: Updated database record",
		}),
		slack.NewSectionBlock(
			&slack.TextBlockObject{
				Type: slack.MarkdownType,
				Text: fmt.Sprintf("*Request ID:* `%s`\n*Updated At:* `%s`\n",
					requestID, time.Now().UTC().Format(time.RFC3339)),
			},
			nil,
			nil,
		),
		slack.NewDividerBlock(),
		slack.NewSectionBlock(
			&slack.TextBlockObject{
				Type: slack.MarkdownType,
				Text: "*Remaining Bucket Paths:*",
			},
			nil,
			nil,
		),
	}
	for _, path := range bp {
		blocks = append(blocks, slack.NewSectionBlock(
			&slack.TextBlockObject{
				Type: slack.MarkdownType,
				Text: fmt.Sprintf("- `%s`", path),
			},
			nil,
			nil,
		))
	}
	blocks = append(blocks, slack.NewDividerBlock(), slack.NewSectionBlock(
		&slack.TextBlockObject{
			Type: slack.MarkdownType,
			Text: "*Processed Paths:*",
		},
		nil,
		nil,
	))
	for _, path := range pp {
		blocks = append(blocks, slack.NewSectionBlock(
			&slack.TextBlockObject{
				Type: slack.MarkdownType,
				Text: fmt.Sprintf("- `%s`", path),
			},
			nil,
			nil,
		))
	}

	if err := sendSlackNotification(os.Getenv("SLACK_CHANNEL_ID"), os.Getenv("SLACK_THREAD_TS"), blocks); err != nil {
		log.Printf("Error sending Slack notification: %v\n", err)
	}

	log.Println("Updated database record:", requestID)

	if len(bp) == 0 {
		deleteQuery := "DELETE FROM restore_requests WHERE request_id = ?"
		_, err = db.Exec(deleteQuery, requestID)
		if err != nil {
			return fmt.Errorf("failed to delete record: %w", err)
		}
		message := fmt.Sprintf(":white_check_mark: *All paths processed for Request ID:* *%s*. *Record deleted.*\n", requestID)
		if err := sendSlackNotification(os.Getenv("SLACK_CHANNEL_ID"), os.Getenv("SLACK_THREAD_TS"), []slack.Block{
			slack.NewSectionBlock(&slack.TextBlockObject{
				Type: slack.MarkdownType,
				Text: message,
			}, nil, nil),
		}); err != nil {
			log.Printf("Error sending Slack notification: %v\n", err)
		}
		fmt.Print(message)
	}

	return nil
}

func restoreObject(svc *s3.Client, bucketName, key string, restoreAction string, sem *semaphore.Weighted, wg *sync.WaitGroup) {
	defer wg.Done()
	defer sem.Release(1)

	log.Printf("Attempting to restore object: %s/%s", bucketName, key)

	restoreRequest := &s3types.RestoreRequest{
		Days: aws.Int32(7),
		GlacierJobParameters: &s3types.GlacierJobParameters{
			Tier: s3types.TierStandard,
		},
	}

	if restoreAction != "" && restoreAction != "standard" {
		days, err := strconv.ParseInt(restoreAction, 10, 32)
		if err == nil {
			restoreRequest.Days = aws.Int32(int32(days))
		} else {
			log.Printf("Invalid restore action passed: %s. Defaulting to 7 days.", restoreAction)
		}
	}

	restoreObjectInput := &s3.RestoreObjectInput{
		Bucket:         aws.String(bucketName),
		Key:            aws.String(key),
		RestoreRequest: restoreRequest,
	}

	_, err := svc.RestoreObject(context.TODO(), restoreObjectInput)
	if err != nil {
		log.Printf("Failed to restore object %s: %v", key, err)
		return
	}

	log.Printf("Object %s has been successfully requested for restoration.", key)
}

func waitForRestoreCompletion(svc *s3.Client, bucketName, key string) error {
	for {
		headInput := &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		}

		headOutput, err := svc.HeadObject(context.TODO(), headInput)
		if err != nil {
			return fmt.Errorf("failed to check object %s status: %v", key, err)
		}

		if headOutput.Restore != nil && strings.Contains(*headOutput.Restore, "true") {
			log.Printf("Restore for object %s is complete.", key)
			return nil
		}

		log.Printf("Object %s is still being restored. Waiting for 15 minutes before retrying...", key)
		time.Sleep(15 * time.Minute)
	}
}

func restoreObjectsInPath(bucketPath, requestID, restoreAction string, failedPaths *[]string, maxConcurrentOps int64, region string) {
	log.Printf("Starting to process bucket path: %s\n", bucketPath)
	parts := strings.SplitN(bucketPath, "/", 2)
	if len(parts) < 2 {
		log.Printf("Invalid bucket path: %s\n", bucketPath)
		*failedPaths = append(*failedPaths, bucketPath)
		return
	}
	bucketName, prefix := parts[0], parts[1]

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credsProvider),
	)
	if err != nil {
		log.Fatalf("Failed to load AWS config: %v\n", err)
	}

	svc := s3.NewFromConfig(cfg)
	sem := semaphore.NewWeighted(maxConcurrentOps)
	var wg sync.WaitGroup

	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	}

	paginator := s3.NewListObjectsV2Paginator(svc, params)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			log.Printf("Failed to list objects for bucket path %s: %v\n", bucketPath, err)
			*failedPaths = append(*failedPaths, bucketPath)
			return
		}

		for _, obj := range page.Contents {
			if obj.StorageClass != s3types.ObjectStorageClassStandard {
				wg.Add(1)
				if err := sem.Acquire(context.Background(), 1); err != nil {
					log.Printf("Failed to acquire semaphore: %v", err)
					wg.Done()
					continue
				}
				go restoreObject(svc, bucketName, *obj.Key, restoreAction, sem, &wg)
			}
		}
	}

	wg.Wait()

	err = updateProcessedPaths(requestID, bucketPath)
	if err != nil {
		log.Printf("Failed to update processed paths for Request ID %s: %v\n", requestID, err)
		*failedPaths = append(*failedPaths, bucketPath)
	}
}

func moveRestoredObjectsToStandard(bucketPath, region string, maxConcurrentOps int64) error {
	log.Printf("Starting to move restored objects to STANDARD storage class for bucket path: %s\n", bucketPath)
	parts := strings.SplitN(bucketPath, "/", 2)
	if len(parts) < 2 {
		return fmt.Errorf("Invalid bucket path: %s", bucketPath)
	}
	bucketName, prefix := parts[0], parts[1]

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credsProvider),
	)
	if err != nil {
		return fmt.Errorf("Failed to load AWS config: %w", err)
	}

	svc := s3.NewFromConfig(cfg)
	sem := semaphore.NewWeighted(maxConcurrentOps)
	var wg sync.WaitGroup

	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	}

	paginator := s3.NewListObjectsV2Paginator(svc, params)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			return fmt.Errorf("Failed to list objects for bucket path %s: %v", bucketPath, err)
		}

		for _, obj := range page.Contents {
			wg.Add(1)
			if err := sem.Acquire(context.Background(), 1); err != nil {
				log.Printf("Failed to acquire semaphore: %v", err)
				wg.Done()
				continue
			}
			go func(objectKey string) {
				defer wg.Done()
				defer sem.Release(1)

				err := waitForRestoreCompletion(svc, bucketName, objectKey)
				if err != nil {
					log.Printf("Error while waiting for restore completion: %v", err)
					return
				}

				_, err = svc.CopyObject(context.TODO(), &s3.CopyObjectInput{
					Bucket:       aws.String(bucketName),
					CopySource:   aws.String(fmt.Sprintf("%s/%s", bucketName, objectKey)),
					Key:          aws.String(objectKey),
					StorageClass: s3types.StorageClassStandard,
				})
				if err != nil {
					log.Printf("Failed to move object %s to STANDARD storage class: %v", objectKey, err)
				} else {
					log.Printf("Object %s successfully moved to STANDARD storage class.", objectKey)
				}
			}(*obj.Key)
		}
	}

	wg.Wait()

	return err
}

func assumeRole(region string) (aws.Credentials, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("failed to load AWS config: %v", err)
	}

	stsSvc := sts.NewFromConfig(cfg)

	callerIdentity, err := stsSvc.GetCallerIdentity(context.TODO(), &sts.GetCallerIdentityInput{})
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("failed to get caller identity: %v", err)
	}

	arn := *callerIdentity.Arn

	roleSessionName := fmt.Sprintf("kubiya-agent-s3-restore-%d", time.Now().Unix())
	credsCache := aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(stsSvc, arn, func(p *stscreds.AssumeRoleOptions) {
		p.RoleSessionName = roleSessionName
	}))

	creds, err := credsCache.Retrieve(context.TODO())
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("failed to assume role: %v", err)
	}

	return creds, nil
}

func renewCredentials(region string) {
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		newCreds, err := assumeRole(region)
		if err != nil {
			log.Printf("Failed to renew credentials: %v", err)
			continue
		}

		credsProvider.UpdateCredentials(newCreds)

		log.Println("Successfully renewed credentials")
	}
}

func main() {
	bucketPaths := flag.String("bucket_paths", "", "Comma-separated list of S3 bucket paths to restore")
	restoreAction := flag.String("restore_action", "", "Specify the restore action. Pass a number of days to restore temporarily, or 'standard' to restore and move objects to STANDARD storage class.")
	maxConcurrentOps := flag.Int64("max_concurrent_ops", 50, "Maximum number of concurrent operations")
	flag.Parse()

	region := os.Getenv("AWS_DEFAULT_REGION")
	if region == "" {
		log.Fatal("AWS_DEFAULT_REGION environment variable is not set")
	}

	if *bucketPaths == "" {
		log.Fatal("Please provide bucket paths")
	}

	initialCreds, err := assumeRole(region)
	if err != nil {
		log.Fatalf("Failed to assume role: %v", err)
	}

	credsProvider = &customCredentialsProvider{creds: &initialCreds}

	go renewCredentials(region)

	requestID := generateRequestID()
	bucketPathsList := strings.Split(*bucketPaths, ",")
	var failedPaths []string

	err = createDBAndRecord(requestID, bucketPathsList)
	if err != nil {
		log.Fatalf("Failed to create DB record: %v\n", err)
	}

	for _, path := range bucketPathsList {
		restoreObjectsInPath(path, requestID, *restoreAction, &failedPaths, *maxConcurrentOps, region)
	}

	if *restoreAction == "standard" {
		for _, path := range bucketPathsList {
			err = moveRestoredObjectsToStandard(path, region, *maxConcurrentOps)
			if err != nil {
				log.Printf("Error while moving objects to STANDARD storage class for path %s: %v", path, err)
				failedPaths = append(failedPaths, path)
			}
		}
	}

	// Prepare the final summary message
	blocks := []slack.Block{
		slack.NewHeaderBlock(&slack.TextBlockObject{
			Type: slack.PlainTextType,
			Text: ":white_check_mark: Restore process completed",
		}),
		slack.NewSectionBlock(
			&slack.TextBlockObject{
				Type: slack.MarkdownType,
				Text: fmt.Sprintf("The restore process for the specified S3 bucket paths has been successfully completed. All objects that required restoration were processed. If you need any further assistance, feel free to ask!\n\n*Request ID:* `%s`\n*Processed Paths:*", requestID),
			},
			nil,
			nil,
		),
		slack.NewDividerBlock(),
	}

	// Add processed paths
	for _, path := range bucketPathsList {
		blocks = append(blocks, slack.NewSectionBlock(
			&slack.TextBlockObject{
				Type: slack.MarkdownType,
				Text: fmt.Sprintf("- `%s`", path),
			},
			nil,
			nil,
		))
	}

	// Add failed paths section
	blocks = append(blocks, slack.NewDividerBlock(), slack.NewSectionBlock(
		&slack.TextBlockObject{
			Type: slack.MarkdownType,
			Text: "*Failed Paths:*",
		},
		nil,
		nil,
	))
	if len(failedPaths) == 0 {
		blocks = append(blocks, slack.NewSectionBlock(
			&slack.TextBlockObject{
				Type: slack.MarkdownType,
				Text: "- None",
			},
			nil,
			nil,
		))
	} else {
		for _, path := range failedPaths {
			blocks = append(blocks, slack.NewSectionBlock(
				&slack.TextBlockObject{
					Type: slack.MarkdownType,
					Text: fmt.Sprintf("- `%s`", path),
				},
				nil,
				nil,
			))
		}
	}

	if err := sendSlackNotification(os.Getenv("SLACK_CHANNEL_ID"), os.Getenv("SLACK_THREAD_TS"), blocks); err != nil {
		log.Printf("Error sending Slack notification: %v\n", err)
	}

	fmt.Printf(":white_check_mark: *Restore process completed for Request ID:* *%s*\n", requestID)
}
