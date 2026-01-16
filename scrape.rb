#!/usr/bin/env ruby
# frozen_string_literal: true

require "bundler/setup"
require "slack-ruby-client"
require "dotenv/load"
require "sqlite3"
require "optparse"
require "set"
require "pg"

DB_PATH = "state.db"
# Captures: domain, index (single digit 0-9), filename
VERCEL_PATTERN = /https:\/\/([a-z0-9-]+-hack-club-bot\.vercel\.app)\/(\d)([^\s<>)"'|]+)/i
BOT_USER_ID = "UM1L1C38X"

class TokenRotator
  attr_accessor :debug

  def initialize(token_str)
    @tokens = token_str.to_s.split(",").map(&:strip).reject(&:empty?)
    @index = 0
    @mutex = Mutex.new
    @debug = false
  end

  def next
    @mutex.synchronize do
      return nil if @tokens.empty?
      token = @tokens[@index]
      idx = @index + 1
      @index = (@index + 1) % @tokens.size
      puts "    [token #{idx}/#{@tokens.size}]" if @debug
      token
    end
  end

  def count
    @tokens.size
  end

  def first
    @tokens.first
  end
end

Options = Struct.new(:limit, :dry_run, :reset, :skip_scrapbook_lookup, :debug)

def parse_options
  options = Options.new(nil, false, false, false, false)
  
  OptionParser.new do |opts|
    opts.banner = "Usage: ruby scrape.rb [options]"
    
    opts.on("-l", "--limit N", Integer, "Limit to N URLs (for testing)") do |n|
      options.limit = n
    end
    
    opts.on("-d", "--dry-run", "Don't write to database, just print URLs") do
      options.dry_run = true
    end
    
    opts.on("-r", "--reset", "Reset progress and start from beginning") do
      options.reset = true
    end
    
    opts.on("-s", "--skip-scrapbook-lookup", "Skip looking up slack files for scrapbook entries") do
      options.skip_scrapbook_lookup = true
    end
    
    opts.on("--debug", "Show token rotation debug info") do
      options.debug = true
    end
    
    opts.on("-h", "--help", "Show this help") do
      puts opts
      exit
    end
  end.parse!
  
  options
end

def init_db
  db = SQLite3::Database.new(DB_PATH)
  db.execute <<-SQL
    CREATE TABLE IF NOT EXISTS files (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      vercel_url TEXT UNIQUE NOT NULL,
      slack_file_url TEXT,
      slack_file_id TEXT,
      filename TEXT,
      hetzner_url TEXT,
      status TEXT DEFAULT 'pending',
      error TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  SQL
  db.execute "CREATE INDEX IF NOT EXISTS idx_status ON files(status)"
  db.execute "CREATE INDEX IF NOT EXISTS idx_vercel_url ON files(vercel_url)"
  
  # Add scrapbook columns if they don't exist
  begin
    db.execute "ALTER TABLE files ADD COLUMN scrapbook_channel TEXT"
  rescue SQLite3::SQLException
    # Column already exists
  end
  begin
    db.execute "ALTER TABLE files ADD COLUMN scrapbook_ts TEXT"
  rescue SQLite3::SQLException
    # Column already exists
  end
  
  # Progress tracking table
  db.execute <<-SQL
    CREATE TABLE IF NOT EXISTS scrape_progress (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      last_page INTEGER DEFAULT 0,
      last_message_ts TEXT,
      total_found INTEGER DEFAULT 0,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  SQL
  
  # Ensure a row exists
  db.execute "INSERT OR IGNORE INTO scrape_progress (id) VALUES (1)"
  
  db
end

def get_progress(db)
  row = db.get_first_row("SELECT last_page, last_message_ts, total_found FROM scrape_progress WHERE id = 1")
  { page: row[0] || 0, last_ts: row[1], total_found: row[2] || 0 }
end

def save_progress(db, page, last_ts, total_found)
  db.execute(
    "UPDATE scrape_progress SET last_page = ?, last_message_ts = ?, total_found = ?, updated_at = CURRENT_TIMESTAMP WHERE id = 1",
    [page, last_ts, total_found]
  )
end

def reset_progress(db)
  db.execute("UPDATE scrape_progress SET last_page = 0, last_message_ts = NULL, total_found = 0, updated_at = CURRENT_TIMESTAMP WHERE id = 1")
  puts "Progress reset."
end

def init_slack
  # Support both SLACK_BOT_TOKENS (comma-separated) and SLACK_BOT_TOKEN (single)
  token_str = ENV["SLACK_BOT_TOKENS"] || ENV["SLACK_BOT_TOKEN"]
  raise "Missing SLACK_BOT_TOKENS or SLACK_BOT_TOKEN" if token_str.nil? || token_str.empty?
  
  rotator = TokenRotator.new(token_str)
  puts "Loaded #{rotator.count} Slack token(s)"
  
  # Don't configure globally - we'll set token per-request
  client = Slack::Web::Client.new
  
  [client, rotator]
end

def extract_vercel_urls(text)
  return [] unless text
  # Returns array of [full_url, index, filename]
  text.scan(VERCEL_PATTERN).map do |domain, index, filename|
    ["https://#{domain}/#{index}#{filename}", index.to_i, filename]
  end
end

def with_retry(max_retries: 5, &block)
  retries = 0
  begin
    yield
  rescue Slack::Web::Api::Errors::TooManyRequestsError => e
    retries += 1
    if retries <= max_retries
      # Parse retry-after from error or default to 10 seconds
      wait_time = e.message.match(/Retry after (\d+)/i)&.[](1)&.to_i || 10
      puts "  Rate limited. Waiting #{wait_time}s (retry #{retries}/#{max_retries})..."
      sleep wait_time + 1
      retry
    else
      raise
    end
  rescue Slack::Web::Api::Errors::TimeoutError, Faraday::TimeoutError => e
    retries += 1
    if retries <= max_retries
      puts "  Timeout. Retrying in 5s (retry #{retries}/#{max_retries})..."
      sleep 5
      retry
    else
      raise
    end
  rescue Slack::Web::Api::Errors::SlackError => e
    retries += 1
    if retries <= max_retries && e.message.include?("timeout")
      puts "  Timeout. Retrying in 5s (retry #{retries}/#{max_retries})..."
      sleep 5
      retry
    else
      raise
    end
  end
end

def process_message(worker_id, client, channel_id, search_msg, seen_urls, db, options, results_mutex, stats)
  vercel_urls = extract_vercel_urls(search_msg.text)
  return if vercel_urls.empty?
  
  # Skip if we've seen all URLs in this message
  new_urls = results_mutex.synchronize { vercel_urls.reject { |url, _, _| seen_urls.include?(url) } }
  return if new_urls.empty?
  
  begin
    msg_response = with_retry do
      client.conversations_replies(
        channel: channel_id,
        ts: search_msg.ts,
        limit: 1
      )
    end
    msg = msg_response.messages&.first
    return unless msg&.thread_ts
    
    parent_response = with_retry do
      client.conversations_replies(
        channel: channel_id,
        ts: msg.thread_ts,
        limit: 1
      )
    end
    parent = parent_response.messages&.first
    files = parent&.files || []
    
    vercel_urls.each do |vercel_url, index, filename|
      results_mutex.synchronize do
        return if options.limit && stats[:total] >= options.limit
        next if seen_urls.include?(vercel_url)
        seen_urls.add(vercel_url)
        
        slack_file = files[index]
        slack_file_url = slack_file&.url_private
        slack_file_id = slack_file&.id
        
        if options.dry_run
          stats[:total] += 1
          status = slack_file ? "✓" : "✗"
          puts "  [W#{worker_id}] #{status} #{vercel_url} -> #{filename}"
        else
          begin
            db.execute(
              "INSERT OR IGNORE INTO files (vercel_url, slack_file_url, slack_file_id, filename) VALUES (?, ?, ?, ?)",
              [vercel_url, slack_file_url, slack_file_id, filename]
            )
            if db.changes > 0
              stats[:total] += 1
              status = slack_file ? "✓" : "✗"
              puts "  [W#{worker_id}] #{status} #{vercel_url} -> #{filename}"
            end
          rescue SQLite3::Exception => e
            puts "  [W#{worker_id}] Error inserting #{vercel_url}: #{e.message}"
          end
        end
      end
    end
    
  rescue Slack::Web::Api::Errors::SlackError => e
    puts "  [W#{worker_id}] Error fetching thread for #{search_msg.ts}: #{e.message}"
  end
end

def search_and_scrape(client, rotator, db, options)
  seen_urls = Set.new
  channel_id = ENV.fetch("SLACK_CHANNEL_ID", "C016DEDUL87")
  
  # Load existing URLs to avoid re-processing
  unless options.dry_run
    db.execute("SELECT vercel_url FROM files").each do |row|
      seen_urls.add(row[0])
    end
    puts "Loaded #{seen_urls.size} existing URLs from database"
  end
  
  # Get resume point
  progress = options.dry_run ? { page: 0, total_found: 0 } : get_progress(db)
  page = progress[:page] + 1  # Start from next page
  stats = { total: progress[:total_found] }
  
  if page > 1
    puts "Resuming from page #{page} (#{stats[:total]} URLs found so far)"
  end
  
  # Create worker pool - one Slack client per token
  num_workers = rotator.count
  work_queue = Queue.new
  results_mutex = Mutex.new
  
  workers = num_workers.times.map do |i|
    Thread.new do
      # Each worker gets its own Slack client with dedicated token
      worker_client = Slack::Web::Client.new
      worker_client.token = rotator.next
      
      while (job = work_queue.pop)
        break if job == :shutdown
        process_message(i + 1, worker_client, channel_id, job, seen_urls, db, options, results_mutex, stats)
      end
    end
  end
  
  puts "Started #{num_workers} workers"

  # Coordinator: search pages and enqueue messages
  loop do
    break if options.limit && stats[:total] >= options.limit
    
    puts "Searching page #{page}..."
    
    # Use first token for search (coordinator)
    client.token = rotator.first
    
    response = with_retry do
      client.search_messages(
        query: "hack-club-bot.vercel.app in:#cdn from:<@#{BOT_USER_ID}>",
        count: 100,
        page: page
      )
    end

    matches = response.messages.matches
    break if matches.empty?
    
    last_ts = nil

    matches.each do |search_msg|
      break if options.limit && stats[:total] >= options.limit
      last_ts = search_msg.ts
      work_queue << search_msg
    end
    
    # Wait for queue to drain before saving progress
    sleep 0.1 until work_queue.empty?
    
    # Save progress after each page
    unless options.dry_run
      save_progress(db, page, last_ts, stats[:total])
    end

    # Check if we've processed all pages
    total_pages = (response.messages.total.to_f / 100).ceil
    break if page >= total_pages
    
    page += 1
  end
  
  # Shutdown workers
  num_workers.times { work_queue << :shutdown }
  workers.each(&:join)

  puts "\nScrape complete!"
  puts "  URLs found this run: #{stats[:total] - progress[:total_found]}"
  puts "  Total URLs: #{stats[:total]}"
  
  unless options.dry_run
    pending = db.get_first_value("SELECT COUNT(*) FROM files WHERE status = 'pending'")
    with_slack = db.get_first_value("SELECT COUNT(*) FROM files WHERE slack_file_url IS NOT NULL")
    without_slack = db.get_first_value("SELECT COUNT(*) FROM files WHERE slack_file_url IS NULL")
    puts "  Pending: #{pending}"
    puts "  With Slack file: #{with_slack}"
    puts "  Missing Slack file: #{without_slack}"
  end
end

def lookup_scrapbook_slack_files(client, rotator, db, options)
  loop do
    # Find files that came from scrapbook but don't have slack URLs yet
    rows = db.execute(<<~SQL)
      SELECT id, vercel_url, scrapbook_channel, scrapbook_ts
      FROM files
      WHERE scrapbook_channel IS NOT NULL
      AND scrapbook_ts IS NOT NULL
      AND (slack_file_url IS NULL OR slack_file_url = '')
      AND status = 'pending'
      LIMIT 1000
    SQL

    break if rows.empty?

    puts "Looking up Slack files for #{rows.size} scrapbook entries..."

    found = 0
    not_found = 0

    rows.each do |row|
      id, vercel_url, channel, ts = row

      # Extract expected filename and index from vercel URL
      match = vercel_url.match(/\/(\d)([^\/]+)$/)
      next unless match
      index = match[1].to_i
      filename = match[2]

      begin
        # Use a small range (±1ms) to account for timestamp precision loss
        ts_float = ts.to_f
        ts_oldest = "%.6f" % (ts_float - 0.001)
        ts_latest = "%.6f" % (ts_float + 0.001)
        client.token = rotator.next
        response = with_retry do
          client.conversations_history(
            channel: channel,
            oldest: ts_oldest,
            latest: ts_latest,
            inclusive: true,
            limit: 1
          )
        end

        msg = response.messages&.first
        files = msg&.files || []

        if files.empty?
          not_found += 1
          next
        end

        # Try to match by index first, then by filename
        slack_file = files[index] || files.find { |f| f.name == filename } || files.first

        if slack_file
          db.execute(
            "UPDATE files SET slack_file_url = ?, slack_file_id = ?, filename = ? WHERE id = ?",
            [slack_file.url_private, slack_file.id, slack_file.name, id]
          )
          found += 1
          puts "  ✓ #{vercel_url} -> #{slack_file.name}"
        else
          not_found += 1
        end

      rescue Slack::Web::Api::Errors::SlackError => e
        puts "  ✗ #{vercel_url}: #{e.message}"
        not_found += 1
      end

      # Small delay to avoid rate limits
      sleep 0.2
    end

    puts "  Found #{found} slack files, #{not_found} not found"
  end
end

def import_from_scrapbook(db)
  scrapbook_url = ENV["SCRAPBOOK_DB_URL"]
  return 0 unless scrapbook_url
  
  puts "Connecting to Scrapbook database..."
  
  # Add sslmode=disable if not present
  unless scrapbook_url.include?("sslmode=")
    scrapbook_url += scrapbook_url.include?("?") ? "&sslmode=disable" : "?sslmode=disable"
  end
  
  begin
    pg = PG.connect(scrapbook_url)
  rescue PG::Error => e
    puts "  Warning: Could not connect to Scrapbook DB: #{e.message}"
    return 0
  end
  
  puts "  Querying Scrapbook for vercel URLs..."
  
  result = pg.exec(<<~SQL)
    SELECT id, channel, "messageTimestamp", attachments
    FROM "Updates"
    WHERE attachments IS NOT NULL
    AND array_length(attachments, 1) > 0
  SQL
  
  imported = 0
  skipped = 0
  
  result.each do |row|
    attachments = row["attachments"]
    channel = row["channel"]
    message_ts = row["messageTimestamp"]
    
    # Parse PostgreSQL array format: {url1,url2,...}
    next unless attachments
    next if channel.nil? || channel.empty? || message_ts.nil?
    
    urls = attachments.gsub(/^\{|\}$/, "").split(",").map(&:strip)
    
    urls.each do |url|
      next unless url.include?("vercel.app")
      
      begin
        db.execute(
          "INSERT OR IGNORE INTO files (vercel_url, scrapbook_channel, scrapbook_ts, status) VALUES (?, ?, ?, 'pending')",
          [url, channel, message_ts]
        )
        if db.changes > 0
          imported += 1
        else
          # Update existing entry with scrapbook info if missing
          db.execute(
            "UPDATE files SET scrapbook_channel = ?, scrapbook_ts = ? WHERE vercel_url = ? AND scrapbook_channel IS NULL",
            [channel, message_ts, url]
          )
          skipped += 1
        end
      rescue SQLite3::Exception => e
        puts "  Error inserting #{url}: #{e.message}"
      end
    end
  end
  
  pg.close
  
  puts "  Imported #{imported} URLs from Scrapbook, skipped #{skipped} duplicates"
  imported
end

def main
  options = parse_options
  
  db = nil
  unless options.dry_run
    puts "Initializing database..."
    db = init_db
    
    if options.reset
      reset_progress(db)
    end
    
    # Import from Scrapbook if SCRAPBOOK_DB_URL is set
    if ENV["SCRAPBOOK_DB_URL"]
      import_from_scrapbook(db)
    end
  end

  puts "Connecting to Slack..."
  client, rotator = init_slack
  rotator.debug = options.debug

  # Look up slack files for scrapbook entries first
  unless options.dry_run || options.skip_scrapbook_lookup
    lookup_scrapbook_slack_files(client, rotator, db, options)
  end

  limit_msg = options.limit ? " (limit: #{options.limit} URLs)" : ""
  dry_run_msg = options.dry_run ? " [DRY RUN]" : ""
  puts "Searching for Vercel URLs#{limit_msg}#{dry_run_msg}..."
  
  search_and_scrape(client, rotator, db, options)
rescue Slack::Web::Api::Errors::SlackError => e
  abort "Slack API error: #{e.message}"
rescue Interrupt
  puts "\nInterrupted. Progress saved."
ensure
  db&.close
end

main if __FILE__ == $PROGRAM_NAME
