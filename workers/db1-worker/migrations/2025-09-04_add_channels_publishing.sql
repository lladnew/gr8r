-- Migration: add Channels + Publishing tables (transaction-free)
-- v1.0.1 (2025-09-04)

-- 1) Reference list of platforms
CREATE TABLE IF NOT EXISTS Channels (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  key           TEXT    NOT NULL UNIQUE,   -- e.g., 'youtube','linkedin'
  display_name  TEXT    NOT NULL           -- e.g., 'YouTube'
);

-- 2) One row per (video × channel)
CREATE TABLE IF NOT EXISTS Publishing (
  id                 INTEGER PRIMARY KEY AUTOINCREMENT,
  video_id           INTEGER NOT NULL,     -- FK to videos.id
  channel_key        TEXT    NOT NULL,     -- logical FK to Channels.key
  scheduled_at       DATETIME,             -- optional per-channel override
  status             TEXT    NOT NULL DEFAULT 'pending',  -- 'pending','queued','posted','failed','skipped'
  platform_media_id  TEXT,                 -- returned ID/URN from platform
  last_error         TEXT,
  posted_at          DATETIME,
  options_json       TEXT,                 -- JSON blob for per-channel payload differences
  record_created     DATETIME NOT NULL DEFAULT (datetime('now')),
  record_modified    DATETIME NOT NULL DEFAULT (datetime('now')),
  UNIQUE (video_id, channel_key),
  FOREIGN KEY (video_id) REFERENCES videos(id) ON DELETE CASCADE
  -- If you want a hard FK to Channels, uncomment below AFTER seeding Channels:
  -- , FOREIGN KEY (channel_key) REFERENCES Channels(key) ON DELETE RESTRICT
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_Publishing_video   ON Publishing (video_id);
CREATE INDEX IF NOT EXISTS idx_Publishing_channel ON Publishing (channel_key);
CREATE INDEX IF NOT EXISTS idx_Publishing_status  ON Publishing (status);

-- Trigger: auto-bump modified time
CREATE TRIGGER IF NOT EXISTS trg_Publishing_mtime
AFTER UPDATE ON Publishing
FOR EACH ROW
BEGIN
  UPDATE Publishing
    SET record_modified = datetime('now')
  WHERE id = NEW.id;
END;

-- Trigger: default per-channel schedule from video if not provided
CREATE TRIGGER IF NOT EXISTS trg_Publishing_default_schedule
AFTER INSERT ON Publishing
FOR EACH ROW
WHEN NEW.scheduled_at IS NULL
BEGIN
  UPDATE Publishing
     SET scheduled_at = (SELECT scheduled_at FROM videos WHERE id = NEW.video_id)
   WHERE id = NEW.id;
END;

-- Seed common channels (edit as needed)
INSERT OR IGNORE INTO Channels (key, display_name) VALUES
  ('youtube',   'YouTube'),
  ('linkedin',  'LinkedIn'),
  ('instagram', 'Instagram'),
  ('threads',   'Threads'),
  ('tiktok',    'TikTok'),
  ('x',         'X'),
  ('facebook',  'Facebook'),
  ('substack',  'Substack');

