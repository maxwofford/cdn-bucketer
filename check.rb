#!/usr/bin/env ruby
# frozen_string_literal: true

require "bundler/setup"
require "dotenv/load"
require "sqlite3"
require "digest"
require "open3"
require "aws-sdk-s3"

DB_PATH = "state.db"
SAMPLE_SIZE = 10

def init_db
  SQLite3::Database.new(DB_PATH)
end

def init_s3_client
  Aws::S3::Client.new(
    endpoint: ENV.fetch("HETZNER_ENDPOINT"),
    region: "auto",
    access_key_id: ENV.fetch("HETZNER_ACCESS_KEY"),
    secret_access_key: ENV.fetch("HETZNER_SECRET_KEY"),
    force_path_style: true
  )
end

def get_random_completed_files(db, limit)
  db.execute(<<~SQL, [limit])
    SELECT id, vercel_url, slack_file_url, hetzner_url, filename
    FROM files
    WHERE status = 'complete'
    AND hetzner_url IS NOT NULL
    AND slack_file_url IS NOT NULL
    ORDER BY RANDOM()
    LIMIT ?
  SQL
end

def download_with_curl(url, auth_header = nil)
  cmd = ["curl", "-sS", "-L", "--fail", "-o", "-"]
  cmd += ["-H", auth_header] if auth_header
  cmd << url
  
  output, status = Open3.capture2(*cmd)
  unless status.success?
    raise "curl failed with exit code #{status.exitstatus}"
  end
  output
end

def download_slack_file(url, token)
  # Convert to download URL (same as main.go)
  download_url = url.sub(%r{/([^/]+)$}, '/download/\1')
  download_with_curl(download_url, "Authorization: Bearer #{token}")
end

def generate_presigned_url(s3_client, bucket, key)
  signer = Aws::S3::Presigner.new(client: s3_client)
  signer.presigned_url(:get_object, bucket: bucket, key: key, expires_in: 300)
end

def extract_s3_key_from_hetzner_url(hetzner_url, public_url)
  # Strip the public URL prefix to get the S3 key
  hetzner_url.sub(public_url.chomp("/") + "/", "")
end

def compute_hash(data)
  Digest::SHA256.hexdigest(data)
end

def main
  # Support both SLACK_BOT_TOKENS (comma-separated) and SLACK_BOT_TOKEN (single)
  token_str = ENV["SLACK_BOT_TOKENS"] || ENV["SLACK_BOT_TOKEN"]
  slack_token = token_str&.split(",")&.first&.strip
  
  unless slack_token && !slack_token.empty?
    abort "Missing SLACK_BOT_TOKENS or SLACK_BOT_TOKEN"
  end
  
  s3_client = init_s3_client
  bucket = ENV.fetch("HETZNER_BUCKET_NAME")
  public_url = ENV.fetch("HETZNER_PUBLIC_URL")
  
  db = init_db
  files = get_random_completed_files(db, SAMPLE_SIZE)
  
  if files.empty?
    puts "No completed files found in database."
    exit 0
  end
  
  puts "Checking #{files.size} random migrated files...\n\n"
  
  passed = 0
  failed = 0
  
  files.each_with_index do |row, idx|
    id, vercel_url, slack_file_url, hetzner_url, filename = row
    
    puts "#{idx + 1}/#{files.size}: #{filename || vercel_url}"
    puts "  Slack:   #{slack_file_url}"
    puts "  Hetzner: #{hetzner_url}"
    
    begin
      # Download original from Slack
      print "  Downloading from Slack... "
      original_data = download_slack_file(slack_file_url, slack_token)
      original_hash = compute_hash(original_data)
      puts "#{original_data.bytesize} bytes"
      
      # Generate presigned URL for Hetzner
      s3_key = extract_s3_key_from_hetzner_url(hetzner_url, public_url)
      presigned_url = generate_presigned_url(s3_client, bucket, s3_key)
      
      # Download migrated file from Hetzner
      print "  Downloading from Hetzner... "
      migrated_data = download_with_curl(presigned_url)
      migrated_hash = compute_hash(migrated_data)
      puts "#{migrated_data.bytesize} bytes"
      
      # Compare
      if original_hash == migrated_hash
        puts "  ✓ MATCH (SHA256: #{original_hash[0..15]}...)"
        passed += 1
      else
        puts "  ✗ MISMATCH!"
        puts "    Original: #{original_hash}"
        puts "    Migrated: #{migrated_hash}"
        failed += 1
      end
      
    rescue => e
      puts "  ✗ ERROR: #{e.message}"
      failed += 1
    end
    
    puts
  end
  
  puts "=" * 50
  puts "Results: #{passed} passed, #{failed} failed"
  
  exit(failed > 0 ? 1 : 0)
ensure
  db&.close
end

main if __FILE__ == $PROGRAM_NAME
