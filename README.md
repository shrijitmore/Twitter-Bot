# 🐦 Twitter Bot - AI-Powered Social Media Automation

A production-ready, optimized Twitter automation bot with AI-powered tweet generation, smart scheduling, and efficient rate limit management for Twitter's free tier (17 tweets/day).

## ✨ Features

### 🤖 AI-Powered Tweet Generation
- **One-time AI Posts**: Generate tweets instantly using Google Gemini AI
- **Multiple Tweet Suggestions**: Get AI-generated ideas for different topics
- **Tone Customization**: Choose from engaging, professional, casual, humorous, or informative tones
- **Content Variety**: AI ensures unique tweets and avoids repetition

### ⏰ Smart Scheduling System
- **Flexible Scheduling**: Every minute, hourly, daily, weekly, or custom cron expressions
- **AI-Generated Scheduled Tweets**: Set topics for AI to create unique daily content
- **Rate Limit Optimization**: Intelligent distribution of 17 daily tweets across 24 hours
- **Queue Management**: Redis-based persistent queue system

### 📊 Advanced Rate Limit Management
- **Real-time Monitoring**: Live display of remaining tweets and reset times
- **Smart Distribution**: Optimal spacing of tweets throughout the day
- **Queue Persistence**: Tweets survive server restarts
- **Retry Logic**: Exponential backoff for failed tweets

### 🎨 Modern UI/UX
- **Beautiful Dashboard**: Gradient design with glassmorphism effects
- **Responsive Design**: Works on desktop and mobile
- **Real-time Updates**: Live rate limit and queue status
- **Interactive Features**: Character counters, form validation, loading states

### 🔧 Production-Ready Architecture
- **Service Layer Pattern**: Proper separation of concerns
- **Error Handling**: Comprehensive error handling and logging
- **Security**: Helmet, rate limiting, input validation
- **Performance**: Compression, caching, optimized queries
- **Monitoring**: Health checks, logging, queue statistics

## 🚀 Quick Start

### Prerequisites
- Node.js 18+ 
- Redis server
- Twitter API credentials (v2)
- Google Gemini API key
- Supabase account (for database)

### Installation

1. **Clone and Install**
   ```bash
   cd /app
   npm install
   ```

2. **Start Redis**
   ```bash
   npm run redis:start
   ```

3. **Configure Environment Variables**
   Update `/app/.env` with your API credentials:
   ```env
   # Supabase Configuration
   SUPABASE_URL=your_supabase_project_url
   SUPABASE_KEY=your_supabase_api_key

   # Twitter API Configuration  
   TWITTER_API_KEY=your_twitter_api_key
   TWITTER_API_SECRET=your_twitter_api_secret
   TWITTER_ACCESS_TOKEN=your_twitter_access_token
   TWITTER_ACCESS_SECRET=your_twitter_access_secret

   # AI Configuration
   GEMINI_API_KEY=your_google_gemini_api_key
   ```

4. **Setup Database Tables**
   Run these SQL commands in your Supabase SQL editor:
   ```sql
   -- Scheduled tweets table
   CREATE TABLE scheduled_tweets (
     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
     text TEXT,
     custom_prompt TEXT,
     schedule_type TEXT NOT NULL,
     cron_time TEXT NOT NULL,
     status TEXT DEFAULT 'scheduled',
     include_image BOOLEAN DEFAULT FALSE,
     image_url TEXT,
     image_prompt TEXT,
     tweet_id TEXT,
     created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
     updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
     sent_at TIMESTAMP WITH TIME ZONE,
     failed_at TIMESTAMP WITH TIME ZONE,
     cancelled_at TIMESTAMP WITH TIME ZONE,
     error_message TEXT
   );

   -- Tweet history table
   CREATE TABLE tweet_history (
     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
     text TEXT NOT NULL,
     tweet_id TEXT,
     has_image BOOLEAN DEFAULT FALSE,
     image_prompt TEXT,
     type TEXT DEFAULT 'manual',
     schedule_type TEXT,
     status TEXT DEFAULT 'success',
     error_message TEXT,
     posted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
   );

   -- Rate limits table (optional, for tracking)
   CREATE TABLE rate_limits (
     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
     limit_type TEXT NOT NULL,
     remaining INTEGER NOT NULL,
     reset_time TIMESTAMP WITH TIME ZONE NOT NULL,
     recorded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
   );

   -- Indexes for performance
   CREATE INDEX idx_scheduled_tweets_status ON scheduled_tweets(status);
   CREATE INDEX idx_scheduled_tweets_created ON scheduled_tweets(created_at);
   CREATE INDEX idx_tweet_history_posted ON tweet_history(posted_at);
   ```

5. **Start the Application**
   ```bash
   npm start
   ```

   Or for development with auto-reload:
   ```bash
   npm run dev
   ```

6. **Access the Dashboard**
   Open http://localhost:8080 in your browser

## 🎭 Demo Mode

The application runs in **Demo Mode** when API keys are not configured, allowing you to:
- ✅ Explore the full UI and features
- ✅ Test AI tweet generation (with mock responses)
- ✅ See how scheduling would work
- ✅ View rate limit management interface

Configure your API keys to enable full functionality.

## 📚 API Documentation

### Health Check
```bash
GET /health
```

### Rate Limit Status
```bash
GET /tweet/rate-status
```

### AI Tweet Generation
```bash
POST /tweet/ai-generate
Content-Type: application/json

{
  "prompt": "Latest AI developments",
  "tone": "engaging",
  "includeImage": false
}
```

### Get AI Suggestions
```bash
GET /tweet/ai-suggestions/tech
```

### Tweet History
```bash
GET /tweet/history?limit=20&offset=0
```

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Frontend      │    │   Express API   │    │   Redis Queue   │
│   (EJS + JS)    │────│   + Services    │────│   + Bull Jobs   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                       ┌─────────────────┐
                       │   Supabase DB   │
                       │   (PostgreSQL)  │  
                       └─────────────────┘
```

### Key Services
- **TwitterService**: Twitter API integration and rate limit management
- **AIService**: Google Gemini AI integration for tweet generation  
- **QueueService**: Redis-based job queue with Bull
- **DatabaseService**: Supabase database operations
- **DemoService**: Mock data for demo mode

## 🔧 Configuration

### Environment Variables
- `PORT`: Server port (default: 8080)
- `NODE_ENV`: Environment (development/production)
- `LOG_LEVEL`: Logging level (info/debug/error)
- `REDIS_HOST`: Redis host (default: localhost)
- `REDIS_PORT`: Redis port (default: 6379)

### Rate Limiting
- Daily tweet limit: 17 (Twitter free tier)
- API requests per minute: 60
- Queue retry attempts: 3-5 with exponential backoff

## 🛠️ Available Scripts

```bash
npm start          # Start production server
npm run dev        # Start development server with nodemon
npm run redis:start # Start Redis server
npm run redis:stop  # Stop Redis server
npm run logs       # View application logs
npm run health     # Check application health
```

## 📊 Monitoring & Logging

- **Structured Logging**: Winston-based logging with multiple transports
- **Health Checks**: `/health` endpoint for monitoring
- **Queue Statistics**: Real-time queue monitoring
- **Error Tracking**: Comprehensive error logging and handling

## 🔒 Security Features

- **Helmet**: Security headers
- **Rate Limiting**: API rate limiting per IP
- **Input Validation**: Joi-based request validation
- **File Upload Security**: Multer with file type restrictions
- **CORS Protection**: Configurable CORS policies

## 🚀 Production Deployment

### Performance Optimizations
- Redis-based persistent queues
- Connection pooling for database
- Response compression
- Optimized database queries
- Smart rate limit distribution

### Security Hardening
- Environment-based configuration
- Secure file upload handling
- Input sanitization and validation
- Rate limiting and DDoS protection

### Monitoring
- Health check endpoints
- Structured logging
- Queue statistics
- Database connection monitoring

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the ISC License.

## 🆘 Support

For support and questions:
1. Check the demo mode first to understand features
2. Verify all environment variables are configured
3. Check logs with `npm run logs`
4. Test health endpoint with `npm run health`

---

**Built with ❤️ for efficient social media automation**