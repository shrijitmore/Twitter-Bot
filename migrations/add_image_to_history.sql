-- Add image-related columns to tweet_history table
ALTER TABLE tweet_history ADD COLUMN IF NOT EXISTS has_image BOOLEAN DEFAULT FALSE;
ALTER TABLE tweet_history ADD COLUMN IF NOT EXISTS image_prompt TEXT;

-- Add index for better query performance
CREATE INDEX IF NOT EXISTS idx_tweet_history_has_image ON tweet_history(has_image);