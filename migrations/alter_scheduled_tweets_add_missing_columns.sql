-- Ensure required columns exist on scheduled_tweets
ALTER TABLE IF EXISTS public.scheduled_tweets
    ADD COLUMN IF NOT EXISTS text TEXT,
    ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMP WITH TIME ZONE;

-- Optional: comment for documentation
COMMENT ON COLUMN public.scheduled_tweets.text IS 'Text content of the scheduled tweet';
COMMENT ON COLUMN public.scheduled_tweets.cancelled_at IS 'When a scheduled tweet was cancelled';

