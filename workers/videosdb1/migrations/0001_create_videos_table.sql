
-- Generated 2025-07-25T19:15:11.821727 UTC
CREATE TABLE videos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'Hold',
  video_type TEXT,
  scheduled_at DATETIME,
  r2_url TEXT,
  r2_transcript_url TEXT,
  video_filename TEXT,
  content_type TEXT,
  file_size_bytes INTEGER,
  transcript_id TEXT,
  planly_media_id TEXT,
  social_copy_hook TEXT,
  social_copy_body TEXT,
  social_copy_cta TEXT,
  hashtags TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME
);
