CREATE TABLE videos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT UNIQUE,
  status TEXT,
  video_type TEXT,
  scheduled_at DATETIME,         -- CHANGED from TEXT
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
  record_created DATETIME,       -- CHANGED from TEXT
  record_modified DATETIME       -- CHANGED from TEXT
);
