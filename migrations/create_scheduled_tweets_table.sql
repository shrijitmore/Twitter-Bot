-- Create scheduled_tweets table
CREATE TABLE IF NOT EXISTS public.scheduled_tweets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    schedule_type TEXT NOT NULL,
    cron_time TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'scheduled',
    custom_prompt TEXT,
    include_image BOOLEAN DEFAULT false,
    image_url TEXT,
    image_prompt TEXT,
    text TEXT,
    tweet_id TEXT,
    error_message TEXT,
    sent_at TIMESTAMP WITH TIME ZONE,
    failed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Add indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_scheduled_tweets_status ON public.scheduled_tweets(status);
CREATE INDEX IF NOT EXISTS idx_scheduled_tweets_created_at ON public.scheduled_tweets(created_at);

-- Add comments for better documentation
COMMENT ON TABLE public.scheduled_tweets IS 'Stores information about scheduled tweets';
COMMENT ON COLUMN public.scheduled_tweets.schedule_type IS 'Type of schedule (everyMinute, hourly, daily, weekly, custom)';
COMMENT ON COLUMN public.scheduled_tweets.cron_time IS 'Cron expression for scheduling';
COMMENT ON COLUMN public.scheduled_tweets.status IS 'Current status of the scheduled tweet (scheduled, sent, failed)';
COMMENT ON COLUMN public.scheduled_tweets.tweet_id IS 'ID of the tweet after it has been posted';

-- Enable Row Level Security
ALTER TABLE public.scheduled_tweets ENABLE ROW LEVEL SECURITY;

-- Create policies for Row Level Security
CREATE POLICY "Enable read access for all users" ON public.scheduled_tweets
    FOR SELECT USING (true);

CREATE POLICY "Enable insert for authenticated users only" ON public.scheduled_tweets
    FOR INSERT WITH CHECK (auth.role() = 'authenticated');

CREATE POLICY "Enable update for authenticated users only" ON public.scheduled_tweets
    FOR UPDATE USING (auth.role() = 'authenticated');

-- Create a trigger to update the updated_at column
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_scheduled_tweets_updated_at
BEFORE UPDATE ON public.scheduled_tweets
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();
