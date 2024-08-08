package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	_ "github.com/mattn/go-sqlite3"
	"github.com/slack-go/slack"
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

func restoreObject(svc *s3.S3, bucketName, key string) error {
	log.Printf("Attempting to restore object: %s/%s", bucketName, key)

	copyInput := &s3.CopyObjectInput{
		Bucket:       aws.String(bucketName),
		CopySource:   aws.String(fmt.Sprintf("%s/%s", bucketName, key)),
		Key:          aws.String(key),
		StorageClass: aws.String("STANDARD"),
	}

	_, err := svc.CopyObject(copyInput)
	if err != nil {
		return fmt.Errorf("failed to restore object %s: %v", key, err)
	}

	// Wait for a few seconds to ensure the object storage class is updated
	time.Sleep(10 * time.Second)

	// Check if the object storage class was updated successfully
	for i := 0; i < 5; i++ {
		headInput := &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		}
		headOutput, err := svc.HeadObject(headInput)
		if err != nil {
			log.Printf("Attempt %d: failed to verify storage class for object %s: %v", i+1, key, err)
		} else {
			log.Printf("Attempt %d: Storage class for object %s is %v", i+1, key, headOutput.StorageClass)
		}

		if headOutput.StorageClass != nil && *headOutput.StorageClass == "STANDARD" {
			log.Printf("Object %s restored to STANDARD storage class\n", key)
			return nil
		}

		// Wait for a few seconds before retrying
		time.Sleep(5 * time.Second)
	}

	// Use ListObjectsV2 to confirm the storage class
	listParams := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(key),
	}

	listOutput, err := svc.ListObjectsV2(listParams)
	if err != nil {
		return fmt.Errorf("failed to list objects for verification: %v", err)
	}

	for _, obj := range listOutput.Contents {
		if *obj.Key == key && obj.StorageClass != nil && *obj.StorageClass == "STANDARD" {
			log.Printf("Object %s restored to STANDARD storage class (verified via ListObjectsV2)\n", key)
			return nil
		}
	}

	return fmt.Errorf("storage class for object %s is not STANDARD, it is <nil>", key)
}

func restoreObjectsInPath(bucketPath, region, requestID string, failedPaths *[]string) {
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

	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	}

	err = svc.ListObjectsV2Pages(params, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		log.Printf("Listing objects in bucket path: %s\n", bucketPath)
		for _, obj := range page.Contents {
			if obj.StorageClass != nil && *obj.StorageClass == "REDUCED_REDUNDANCY" {
				err := restoreObject(svc, bucketName, *obj.Key)
				if err != nil {
					log.Printf("Error restoring object %s: %v\n", *obj.Key, err)
					*failedPaths = append(*failedPaths, *obj.Key)
					continue
				}
				// Wait for a few seconds to ensure the object is processed before moving on to the next
				time.Sleep(2 * time.Second)
			}
		}
		return true
	})

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
	region := flag.String("region", "", "AWS region")
	flag.Parse()

	if *bucketPaths == "" {
		log.Fatal("Please provide bucket paths")
	}
	if *region == "" {
		log.Fatal("Please provide an AWS region")
	}

	requestID := generateRequestID()
	bucketPathsList := strings.Split(*bucketPaths, ",")
	var failedPaths []string

	err := createDBAndRecord(requestID, bucketPathsList)
	if err != nil {
		log.Fatalf("Failed to create DB record: %v\n", err)
	}

	for _, path := range bucketPathsList {
		restoreObjectsInPath(path, *region, requestID, &failedPaths)
	}

	if len(failedPaths) > 0 {
		failedPathsJSON, _ := json.Marshal(failedPaths)
		message := fmt.Sprintf(":x: *The following paths failed to be processed for Request ID:* *%s*\n*Failed Paths:* `%s`\n", requestID, failedPathsJSON)
		if err := sendSlackNotification(os.Getenv("SLACK_CHANNEL_ID"), os.Getenv("SLACK_THREAD_TS"), []slack.Block{
			slack.NewSectionBlock(&slack.TextBlockObject{
				Type: slack.MarkdownType,
				Text: message,
			}, nil, nil),
		}); err != nil {
			log.Printf("Error sending Slack notification for failed paths: %v\n", err)
		}
		log.Println(message)
	}

	fmt.Printf(":white_check_mark: *Restore process completed for Request ID:* *%s*\n", requestID)
}
