
-- Migration to add timestamp tracking fields
ALTER TABLE videos ADD COLUMN record_created DATETIME;
ALTER TABLE videos ADD COLUMN record_modified DATETIME;
