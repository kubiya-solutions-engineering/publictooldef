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
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
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

var messageTimestamp string

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
	VALUES (?, ?, ?, ?, ?, ?)`
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

func restoreObject(svc *s3.S3, bucketName, key string, restoreAction string, sem *semaphore.Weighted, wg *sync.WaitGroup) {
	defer wg.Done()
	defer sem.Release(1)

	log.Printf("Attempting to restore object: %s/%s", bucketName, key)

	restoreRequest := &s3.RestoreObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		RestoreRequest: &s3.RestoreRequest{
			Days: aws.Int64(7),
			GlacierJobParameters: &s3.GlacierJobParameters{
				Tier: aws.String("Standard"),
			},
		},
	}

	if restoreAction != "standard" {
		days, err := strconv.ParseInt(restoreAction, 10, 64)
		if err == nil {
			restoreRequest.RestoreRequest.Days = aws.Int64(days)
		} else {
			log.Printf("Invalid restore action passed: %s. Defaulting to 7 days.", restoreAction)
		}
	} else {
		restoreRequest.RestoreRequest.Days = nil
	}

	_, err := svc.RestoreObject(restoreRequest)
	if err != nil {
		log.Printf("Failed to restore object %s: %v", key, err)
		return
	}

	log.Printf("Object %s has been successfully restored.", key)

	if restoreAction == "standard" {
		err := waitForRestoreCompletion(svc, bucketName, key)
		if err != nil {
			log.Printf("Error while waiting for restore completion: %v", err)
			return
		}

		copyInput := &s3.CopyObjectInput{
			Bucket:       aws.String(bucketName),
			CopySource:   aws.String(fmt.Sprintf("%s/%s", bucketName, key)),
			Key:          aws.String(key),
			StorageClass: aws.String("STANDARD"),
		}

		_, err = svc.CopyObject(copyInput)
		if err != nil {
			log.Printf("Failed to move object %s to STANDARD storage class: %v", key, err)
		} else {
			log.Printf("Object %s successfully moved to STANDARD storage class.", key)
		}
	}
}

func waitForRestoreCompletion(svc *s3.S3, bucketName, key string) error {
	for {
		headInput := &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		}

		headOutput, err := svc.HeadObject(headInput)
		if err != nil {
			return fmt.Errorf("failed to check object %s status: %v", key, err)
		}

		if headOutput.Restore != nil && strings.Contains(*headOutput.Restore, "true") {
			log.Printf("Restore for object %s is complete.", key)
			return nil
		}

		log.Printf("Object %s is still being restored. Waiting for 30 seconds before retrying...", key)
		time.Sleep(30 * time.Second)
	}
}

func restoreObjectsInPath(bucketPath, region, requestID, restoreAction string, failedPaths *[]string, maxConcurrentOps int64) {
	log.Printf("Starting to process bucket path: %s\n", bucketPath)
	parts := strings.SplitN(bucketPath, "/", 2)
	if len(parts) < 2 {
		log.Printf("Invalid bucket path: %s\n", bucketPath)
		*failedPaths = append(*failedPaths, bucketPath)
		return
	}
	bucketName, prefix := parts[0], parts[1]

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)
	if err != nil {
		log.Fatalf("Failed to create session: %v\n", err)
	}

	svc := s3.New(sess)
	sem := semaphore.NewWeighted(maxConcurrentOps)
	var wg sync.WaitGroup

	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	}

	err = svc.ListObjectsV2Pages(params, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		log.Printf("Listing objects in bucket path: %s\n", bucketPath)
		for _, obj := range page.Contents {
			if obj.StorageClass != nil && (*obj.StorageClass == "DEEP_ARCHIVE" || *obj.StorageClass == "GLACIER") {
				wg.Add(1)
				if err := sem.Acquire(context.Background(), 1); err != nil {
					log.Printf("Failed to acquire semaphore: %v", err)
					wg.Done()
					continue
				}
				go restoreObject(svc, bucketName, *obj.Key, restoreAction, sem, &wg)
			}
		}
		return true
	})

	wg.Wait()

	if err != nil {
		log.Printf("Failed to list objects for bucket path %s: %v\n", bucketPath, err)
		*failedPaths = append(*failedPaths, bucketPath)
		return
	}

	err = updateProcessedPaths(requestID, bucketPath)
	if err != nil {
		log.Printf("Failed to update processed paths for Request ID %s: %v\n", requestID, err)
		*failedPaths = append(*failedPaths, bucketPath)
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

	requestID := generateRequestID()
	bucketPathsList := strings.Split(*bucketPaths, ",")
	var failedPaths []string

	err := createDBAndRecord(requestID, bucketPathsList)
	if err != nil {
		log.Fatalf("Failed to create DB record: %v\n", err)
	}

	for _, path := range bucketPathsList {
		restoreObjectsInPath(path, region, requestID, *restoreAction, &failedPaths, *maxConcurrentOps)
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
