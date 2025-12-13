# CDN Bucketer

Migrate files from Slack (originally served via Vercel CDN) to Hetzner Object Storage, preserving the original Vercel URL structure.

## Architecture

```
┌─────────────┐      ┌───────────┐      ┌─────────────────┐
│  Slack API  │ ───► │  SQLite   │ ───► │  Hetzner S3     │
│  (Ruby)     │      │  state.db │      │  (Go workers)   │
└─────────────┘      └───────────┘      └─────────────────┘
     scrape.rb            ▲                   main.go
        │                 │                       │
   Search for         Resumable              Download from
   Vercel URLs,         state               Slack, upload
   map to Slack                              to Hetzner
   files
```

## Setup

1. Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

2. Install Ruby dependencies:

```bash
bundle install
```

3. Build the Go migrator:

```bash
go build -o migrate .
```

## Usage

### Step 1: Scrape Slack for Vercel URLs

```bash
# Test first
ruby scrape.rb --limit 100 --dry-run

# Full scrape
ruby scrape.rb
```

This will:
- Search Slack for messages containing `*-hack-club-bot.vercel.app` URLs
- Find the parent message with the original Slack files
- Map each Vercel URL to its corresponding Slack file (by index)
- Store in `state.db` with status `pending`

### Step 2: Download & Upload to Hetzner

```bash
./migrate
```

This will:
- Read pending URLs from `state.db`
- Download files from Slack (with auth, concurrent workers)
- Upload to Hetzner S3 (streaming, memory-efficient)
- Preserve Vercel URL structure in S3 keys (e.g., `cloud-abc123/0filename.png`)
- Update status in `state.db`
- Export `url_mapping.csv` when complete

### Resuming

Just run `./migrate` again. It will:
- Reset any stuck `processing` files to `pending`
- Skip already `complete` files
- Retry `error` files (change status to `pending` in DB if needed)

### Check Status

```bash
sqlite3 state.db "SELECT status, COUNT(*) FROM files GROUP BY status;"
```

### Retry Errors

```bash
sqlite3 state.db "UPDATE files SET status = 'pending' WHERE status = 'error';"
./migrate
```

## Output

- `state.db` - SQLite database with all state
- `url_mapping.csv` - CSV with columns: `vercel_url`, `hetzner_url`, `status`

## Configuration

| Variable | Description |
|----------|-------------|
| `SLACK_BOT_TOKEN` | Slack user token (xoxp) with `search:read`, `channels:history`, `files:read` scopes |
| `SLACK_CHANNEL_ID` | Channel ID to scrape (default: C016DEDUL87) |
| `HETZNER_ENDPOINT` | S3 endpoint (e.g., `https://fsn1.your-objectstorage.com`) |
| `HETZNER_ACCESS_KEY` | Access key |
| `HETZNER_SECRET_KEY` | Secret key |
| `HETZNER_BUCKET_NAME` | Bucket name |
| `HETZNER_PUBLIC_URL` | Public URL prefix for the bucket |
| `DOWNLOAD_WORKERS` | Number of concurrent download/upload workers (default: 10) |
