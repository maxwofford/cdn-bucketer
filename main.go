package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/joho/godotenv"
	_ "modernc.org/sqlite"
)

type File struct {
	ID           int64
	VercelURL    string
	SlackFileURL string
	Filename     string
}

type UploadResult struct {
	ID         int64
	VercelURL  string
	HetznerURL string
	Error      error
}

type Config struct {
	HetznerEndpoint  string
	HetznerAccessKey string
	HetznerSecretKey string
	HetznerBucket    string
	HetznerPublicURL string
	SlackTokens      *TokenRotator
	DownloadWorkers  int
	UploadWorkers    int
}

type TokenRotator struct {
	tokens []string
	index  int
	mu     sync.Mutex
}

func NewTokenRotator(tokenStr string) *TokenRotator {
	tokens := strings.Split(tokenStr, ",")
	cleaned := make([]string, 0, len(tokens))
	for _, t := range tokens {
		t = strings.TrimSpace(t)
		if t != "" {
			cleaned = append(cleaned, t)
		}
	}
	return &TokenRotator{tokens: cleaned}
}

func (r *TokenRotator) Next() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.tokens) == 0 {
		return ""
	}
	token := r.tokens[r.index]
	r.index = (r.index + 1) % len(r.tokens)
	return token
}

func (r *TokenRotator) Count() int {
	return len(r.tokens)
}

func loadConfig() (*Config, error) {
	_ = godotenv.Load()

	downloadWorkers, _ := strconv.Atoi(os.Getenv("DOWNLOAD_WORKERS"))
	if downloadWorkers == 0 {
		downloadWorkers = 3 // Conservative default to avoid Slack rate limits
	}
	uploadWorkers, _ := strconv.Atoi(os.Getenv("UPLOAD_WORKERS"))
	if uploadWorkers == 0 {
		uploadWorkers = 10
	}

	slackTokens := NewTokenRotator(os.Getenv("SLACK_BOT_TOKEN"))

	cfg := &Config{
		HetznerEndpoint:  os.Getenv("HETZNER_ENDPOINT"),
		HetznerAccessKey: os.Getenv("HETZNER_ACCESS_KEY"),
		HetznerSecretKey: os.Getenv("HETZNER_SECRET_KEY"),
		HetznerBucket:    os.Getenv("HETZNER_BUCKET_NAME"),
		HetznerPublicURL: os.Getenv("HETZNER_PUBLIC_URL"),
		SlackTokens:      slackTokens,
		DownloadWorkers:  downloadWorkers,
		UploadWorkers:    uploadWorkers,
	}

	if cfg.HetznerEndpoint == "" || cfg.HetznerAccessKey == "" || cfg.HetznerSecretKey == "" || cfg.HetznerBucket == "" {
		return nil, fmt.Errorf("missing required Hetzner configuration")
	}
	if slackTokens.Count() == 0 {
		return nil, fmt.Errorf("missing SLACK_BOT_TOKEN")
	}

	return cfg, nil
}

func createS3Client(cfg *Config) *s3.Client {
	return s3.New(s3.Options{
		BaseEndpoint: aws.String(cfg.HetznerEndpoint),
		Region:       "auto",
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.HetznerAccessKey, cfg.HetznerSecretKey, ""),
	})
}

func openDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite", "state.db")
	if err != nil {
		return nil, err
	}
	// Enable WAL mode for better concurrent access
	_, err = db.Exec("PRAGMA journal_mode=WAL")
	if err != nil {
		return nil, err
	}
	return db, nil
}

func getPendingFiles(db *sql.DB, limit int) ([]File, error) {
	rows, err := db.Query(
		`SELECT id, vercel_url, slack_file_url, filename 
		 FROM files 
		 WHERE status = 'pending' AND slack_file_url IS NOT NULL AND slack_file_url != ''
		 LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []File
	for rows.Next() {
		var f File
		if err := rows.Scan(&f.ID, &f.VercelURL, &f.SlackFileURL, &f.Filename); err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	return files, rows.Err()
}

func markProcessing(db *sql.DB, id int64) error {
	_, err := db.Exec(
		"UPDATE files SET status = 'processing', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
		id,
	)
	return err
}

func markComplete(db *sql.DB, id int64, hetznerURL string) error {
	_, err := db.Exec(
		"UPDATE files SET status = 'complete', hetzner_url = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
		hetznerURL, id,
	)
	return err
}

func markError(db *sql.DB, id int64, errMsg string) error {
	_, err := db.Exec(
		"UPDATE files SET status = 'error', error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
		errMsg, id,
	)
	return err
}

func resetStuckProcessing(db *sql.DB) (int64, error) {
	result, err := db.Exec(
		"UPDATE files SET status = 'pending' WHERE status = 'processing'",
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// convertToDownloadURL converts a Slack files-pri URL to the download variant
// e.g., .../files-pri/T.../filename.png -> .../files-pri/T.../download/filename.png
func convertToDownloadURL(fileURL string) string {
	// Insert /download/ before the filename
	lastSlash := strings.LastIndex(fileURL, "/")
	if lastSlash == -1 {
		return fileURL
	}
	return fileURL[:lastSlash] + "/download" + fileURL[lastSlash:]
}

// downloadFile downloads a file from Slack and returns a reader, content type, and size
func downloadFile(ctx context.Context, fileURL string, slackToken string) (io.ReadCloser, string, int64, error) {
	client := &http.Client{Timeout: 5 * time.Minute}
	maxRetries := 5
	
	// Convert to download URL for Enterprise Grid compatibility
	downloadURL := convertToDownloadURL(fileURL)
	
	for attempt := 0; attempt < maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "GET", downloadURL, nil)
		if err != nil {
			return nil, "", 0, err
		}
		
		// Slack private files require auth
		req.Header.Set("Authorization", "Bearer "+slackToken)

		resp, err := client.Do(req)
		if err != nil {
			return nil, "", 0, err
		}

		if resp.StatusCode == http.StatusOK {
			contentType := resp.Header.Get("Content-Type")
			if contentType == "" {
				contentType = "application/octet-stream"
			}
			return resp.Body, contentType, resp.ContentLength, nil
		}
		
		resp.Body.Close()
		
		// Handle rate limiting (429)
		if resp.StatusCode == http.StatusTooManyRequests {
			retryAfter := resp.Header.Get("Retry-After")
			waitSecs := 10
			if retryAfter != "" {
				if parsed, err := strconv.Atoi(retryAfter); err == nil {
					waitSecs = parsed
				}
			}
			// Exponential backoff
			waitTime := time.Duration(waitSecs*(attempt+1)) * time.Second
			select {
			case <-ctx.Done():
				return nil, "", 0, ctx.Err()
			case <-time.After(waitTime):
				continue
			}
		}
		
		return nil, "", 0, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	
	return nil, "", 0, fmt.Errorf("max retries exceeded (429)")
}

// generateS3Key creates a unique key from the Vercel URL, preserving original structure
func generateS3Key(vercelURL string, filename string) string {
	parsed, err := url.Parse(vercelURL)
	if err != nil {
		// Fallback to hash
		hash := sha256.Sum256([]byte(vercelURL))
		return hex.EncodeToString(hash[:])
	}

	// Extract subdomain for organization (e.g., "cloud-abc123" from "cloud-abc123-hack-club-bot.vercel.app")
	subdomain := strings.Split(parsed.Host, "-hack-club-bot")[0]
	
	// Get the index from the path (e.g., "0" from "/0filename.png")
	pathPart := strings.TrimPrefix(parsed.Path, "/")
	
	// Use provided filename, fallback to path-based extraction
	if filename == "" {
		filename = path.Base(parsed.Path)
	}
	if filename == "" || filename == "/" {
		hash := sha256.Sum256([]byte(vercelURL))
		filename = hex.EncodeToString(hash[:8])
	}

	// Structure: subdomain/index+filename (matches original Vercel structure)
	return fmt.Sprintf("%s/%s", subdomain, pathPart)
}

func uploadToS3(ctx context.Context, client *s3.Client, bucket string, key string, body io.Reader, contentType string, contentLength int64) error {
	input := &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        body,
		ContentType: aws.String(contentType),
	}
	
	if contentLength > 0 {
		input.ContentLength = aws.Int64(contentLength)
	}

	_, err := client.PutObject(ctx, input)
	return err
}

func worker(
	ctx context.Context,
	id int,
	files <-chan File,
	results chan<- UploadResult,
	s3Client *s3.Client,
	cfg *Config,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case file, ok := <-files:
			if !ok {
				return
			}

			result := UploadResult{
				ID:        file.ID,
				VercelURL: file.VercelURL,
			}

			// Download from Slack (not Vercel) - rotate through tokens
			slackToken := cfg.SlackTokens.Next()
			body, contentType, contentLength, err := downloadFile(ctx, file.SlackFileURL, slackToken)
			if err != nil {
				result.Error = fmt.Errorf("download failed: %w", err)
				results <- result
				continue
			}

			// Generate S3 key preserving Vercel URL structure
			key := generateS3Key(file.VercelURL, file.Filename)

			// Upload
			err = uploadToS3(ctx, s3Client, cfg.HetznerBucket, key, body, contentType, contentLength)
			body.Close()

			if err != nil {
				result.Error = fmt.Errorf("upload failed: %w", err)
				results <- result
				continue
			}

			// Build public URL
			result.HetznerURL = fmt.Sprintf("%s/%s", strings.TrimRight(cfg.HetznerPublicURL, "/"), key)
			results <- result
		}
	}
}

func exportCSV(db *sql.DB, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Header
	if err := writer.Write([]string{"vercel_url", "hetzner_url", "status"}); err != nil {
		return err
	}

	rows, err := db.Query("SELECT vercel_url, COALESCE(hetzner_url, ''), status FROM files")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var vercelURL, hetznerURL, status string
		if err := rows.Scan(&vercelURL, &hetznerURL, &status); err != nil {
			return err
		}
		if err := writer.Write([]string{vercelURL, hetznerURL, status}); err != nil {
			return err
		}
	}

	return rows.Err()
}

func printStats(db *sql.DB) {
	var pending, processing, complete, errored int
	db.QueryRow("SELECT COUNT(*) FROM files WHERE status = 'pending'").Scan(&pending)
	db.QueryRow("SELECT COUNT(*) FROM files WHERE status = 'processing'").Scan(&processing)
	db.QueryRow("SELECT COUNT(*) FROM files WHERE status = 'complete'").Scan(&complete)
	db.QueryRow("SELECT COUNT(*) FROM files WHERE status = 'error'").Scan(&errored)

	fmt.Printf("\nStatus: pending=%d, processing=%d, complete=%d, errors=%d\n",
		pending, processing, complete, errored)
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Config error: %v\n", err)
		os.Exit(1)
	}

	db, err := openDB()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Database error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	fmt.Printf("Loaded %d Slack bot tokens for rotation\n", cfg.SlackTokens.Count())

	// Reset any files stuck in 'processing' state from previous run
	reset, _ := resetStuckProcessing(db)
	if reset > 0 {
		fmt.Printf("Reset %d files from 'processing' to 'pending'\n", reset)
	}

	printStats(db)

	s3Client := createS3Client(cfg)

	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown gracefully
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nShutting down gracefully...")
		cancel()
	}()

	// Channels
	fileChan := make(chan File, 100)
	resultChan := make(chan UploadResult, 100)

	// Start workers
	var wg sync.WaitGroup
	numWorkers := cfg.DownloadWorkers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(ctx, i, fileChan, resultChan, s3Client, cfg, &wg)
	}

	// Result processor
	var resultWg sync.WaitGroup
	resultWg.Add(1)
	go func() {
		defer resultWg.Done()
		for result := range resultChan {
			if result.Error != nil {
				fmt.Printf("✗ [%d] %s: %v\n", result.ID, result.VercelURL, result.Error)
				markError(db, result.ID, result.Error.Error())
			} else {
				fmt.Printf("✓ [%d] %s → %s\n", result.ID, result.VercelURL, result.HetznerURL)
				markComplete(db, result.ID, result.HetznerURL)
			}
		}
	}()

	// Feed files to workers
	batchSize := 100
	for {
		select {
		case <-ctx.Done():
			goto shutdown
		default:
		}

		files, err := getPendingFiles(db, batchSize)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error fetching files: %v\n", err)
			time.Sleep(time.Second)
			continue
		}

		if len(files) == 0 {
			fmt.Println("No more pending files.")
			break
		}

		for _, f := range files {
			select {
			case <-ctx.Done():
				goto shutdown
			default:
				markProcessing(db, f.ID)
				fileChan <- f
			}
		}
	}

shutdown:
	close(fileChan)
	wg.Wait()
	close(resultChan)
	resultWg.Wait()

	printStats(db)

	// Export CSV
	csvFile := "url_mapping.csv"
	if err := exportCSV(db, csvFile); err != nil {
		fmt.Fprintf(os.Stderr, "Error exporting CSV: %v\n", err)
	} else {
		fmt.Printf("Exported mapping to %s\n", csvFile)
	}
}
