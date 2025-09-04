const { TwitterApi } = require('twitter-api-v2');
const config = require('../config');
const logger = require('../utils/logger');
const queueService = require('./QueueService');
const dotenv = require('dotenv');
dotenv.config();

class TwitterService {
  constructor() {
    this.client = new TwitterApi({
      appKey: config.twitter.apiKey,
      appSecret: config.twitter.apiSecret,
      accessToken: config.twitter.accessToken,
      accessSecret: config.twitter.accessSecret,
    });

    this.rateLimitStatus = {
      daily: {
        limit: config.rateLimits.dailyTweetLimit,
        remaining: config.rateLimits.dailyTweetLimit,
        reset: 0,
        lastUpdated: 0,
      },
    };
  }

  async postTweet(text, mediaId = null) {
    try {
      // Check if we can post (daily limit)
      const canPost = await queueService.canProcessTweet();
      if (!canPost) {
        const dailyCount = await queueService.getDailyCount();
        throw new Error(`Daily tweet limit reached (${dailyCount}/${config.rateLimits.dailyTweetLimit}). Please wait for reset.`);
      }

      const tweetOptions = {};
      if (mediaId) {
        tweetOptions.media = { media_ids: [mediaId] };
      }

      const result = await this.client.v2.tweet(text, tweetOptions);
      
      // Increment daily count
      await queueService.incrementDailyCount();
      
      logger.info('Tweet posted successfully:', {
        tweetId: result.data.id,
        text: text.substring(0, 50) + '...',
      });

      return result;
    } catch (error) {
      logger.error('Failed to post tweet:', error.message);
      logger.error('Error details:', { 
        message: error.message, 
        code: error.code, 
        status: error.status,
        type: typeof error,
        keys: Object.keys(error)
      });
      
      // Provide more specific error messages
      if (error.message.includes('demo') || error.message.includes('credentials')) {
        const newError = new Error('Twitter API not configured. Please set Twitter API credentials in your .env file.');
        newError.code = error.code;
        throw newError;
      } else if (error.code === 401 || error.message.includes('Unauthorized')) {
        const newError = new Error('Twitter API credentials are invalid. Please check your API keys and tokens.');
        newError.code = error.code;
        throw newError;
      } else if (error.code === 429 || error.message.includes('rate limit')) {
        await this.updateRateLimitStatus();
        const newError = new Error('Twitter API rate limit exceeded. Please wait before posting again.');
        newError.code = error.code;
        throw newError;
      } else if (error.code === 403 || error.message.includes('Forbidden')) {
        const newError = new Error('Twitter API access forbidden. Please check your app permissions.');
        newError.code = error.code;
        throw newError;
      } else {
        // Include the original error message for better debugging
        const newError = new Error(`Twitter API error: ${error.message}`);
        newError.code = error.code;
        throw newError;
      }
    }
  }

  async uploadMedia(filePath) {
    try {
      const mediaId = await this.client.v1.uploadMedia(filePath);
      logger.info('Media uploaded successfully:', mediaId);
      return mediaId;
    } catch (error) {
      logger.error('Failed to upload media:', error.message);
      throw error;
    }
  }

  async updateRateLimitStatus() {
    try {
      // For Twitter API v2, we track daily limits manually since free tier has 17 tweets/day
      const dailyCount = await queueService.getDailyCount();
      const remaining = Math.max(0, config.rateLimits.dailyTweetLimit - dailyCount);
      
      // Calculate reset time (midnight UTC)
      const now = new Date();
      const tomorrow = new Date(now);
      tomorrow.setUTCDate(tomorrow.getUTCDate() + 1);
      tomorrow.setUTCHours(0, 0, 0, 0);
      
      this.rateLimitStatus = {
        daily: {
          limit: config.rateLimits.dailyTweetLimit,
          remaining: remaining,
          reset: tomorrow.getTime(),
          lastUpdated: Date.now(),
        },
      };

      logger.info('Rate limit status updated:', {
        remaining,
        limit: config.rateLimits.dailyTweetLimit,
        resetTime: tomorrow.toISOString(),
      });

      return this.rateLimitStatus;
    } catch (error) {
      logger.error('Failed to update rate limit status:', error);
      return null;
    }
  }

  async getRateLimitStatus() {
    try {
      // Update rate limit status if it's been more than 5 minutes
      const timeSinceUpdate = Date.now() - this.rateLimitStatus.daily.lastUpdated;
      if (timeSinceUpdate > 5 * 60 * 1000) { // 5 minutes
        await this.updateRateLimitStatus();
      }

      const now = Date.now();
      const resetTime = new Date(this.rateLimitStatus.daily.reset);
      const timeUntilReset = Math.max(0, this.rateLimitStatus.daily.reset - now);

      return {
        success: true,
        limit: this.rateLimitStatus.daily.limit,
        remaining: this.rateLimitStatus.daily.remaining,
        reset: this.rateLimitStatus.daily.reset,
        resetTime: resetTime.toISOString(),
        timeUntilReset: timeUntilReset,
        status: this.rateLimitStatus.daily.remaining > 0 ? 'OK' : 'RATE_LIMIT_REACHED',
        message: this.rateLimitStatus.daily.remaining > 0 
          ? `You have ${this.rateLimitStatus.daily.remaining} tweets remaining today.`
          : `Daily limit reached. Resets in ${Math.ceil(timeUntilReset / (1000 * 60 * 60))} hours.`,
      };
    } catch (error) {
      logger.error('Failed to get rate limit status:', error);
      return {
        success: false,
        error: error.message,
      };
    }
  }

  // Verify Twitter API credentials
  async verifyCredentials() {
    try {
      const user = await this.client.v2.me();
      logger.info('Twitter API credentials verified for user:', user.data.username);
      return true;
    } catch (error) {
      logger.error('Failed to verify Twitter credentials:', error.message);
      return false;
    }
  }

  // Calculate optimal tweet distribution throughout the day
  calculateOptimalSchedule(totalTweets) {
    const hoursInDay = 24;
    const tweetsPerHour = Math.floor(totalTweets / hoursInDay);
    const remainder = totalTweets % hoursInDay;
    
    const schedule = [];
    for (let hour = 0; hour < hoursInDay; hour++) {
      const tweetsThisHour = tweetsPerHour + (hour < remainder ? 1 : 0);
      if (tweetsThisHour > 0) {
        schedule.push({
          hour,
          tweets: tweetsThisHour,
          delays: this.calculateHourlyDelays(tweetsThisHour),
        });
      }
    }
    
    return schedule;
  }

  calculateHourlyDelays(tweetsInHour) {
    if (tweetsInHour === 1) return [0];
    
    const delays = [];
    const intervalMinutes = 60 / tweetsInHour;
    
    for (let i = 0; i < tweetsInHour; i++) {
      delays.push(Math.floor(i * intervalMinutes));
    }
    
    return delays;
  }
}

module.exports = new TwitterService();