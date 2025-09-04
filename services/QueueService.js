const Queue = require('bull');
const Redis = require('redis');
const config = require('../config');
const logger = require('../utils/logger');

class QueueService {
  constructor() {
    this.redisClient = Redis.createClient({
      socket: {
        host: config.redis.host,
        port: config.redis.port,
      },
      password: config.redis.password,
    });

    this.redisClient.on('error', (err) => {
      logger.error('Redis Client Error:', err);
    });

    this.redisClient.on('connect', () => {
      logger.info('Connected to Redis');
    });

    // Connect to Redis
    this.redisClient.connect().catch(err => {
      logger.error('Failed to connect to Redis:', err);
    });

    // Create different queues for different types of jobs
    this.tweetQueue = new Queue('tweet processing', {
      redis: {
        port: config.redis.port,
        host: config.redis.host,
        password: config.redis.password,
      },
      defaultJobOptions: {
        removeOnComplete: 100,
        removeOnFail: 50,
        attempts: 3,
        backoff: {
          type: 'exponential',
          delay: 2000,
        },
      },
    });

    this.scheduledTweetQueue = new Queue('scheduled tweets', {
      redis: {
        port: config.redis.port,
        host: config.redis.host,
        password: config.redis.password,
      },
      defaultJobOptions: {
        removeOnComplete: 50,
        removeOnFail: 25,
        attempts: 5,
        backoff: {
          type: 'exponential',
          delay: 5000,
        },
      },
    });

    this.dailyQuotaResetQueue = new Queue('daily quota reset', {
      redis: {
        port: config.redis.port,
        host: config.redis.host,
        password: config.redis.password,
      },
    });

    this.setupQueueEvents();
  }

  setupQueueEvents() {
    // Tweet Queue Events
    this.tweetQueue.on('completed', (job, result) => {
      logger.info(`Tweet job ${job.id} completed:`, result);
    });

    this.tweetQueue.on('failed', (job, err) => {
      logger.error(`Tweet job ${job.id} failed:`, err.message);
    });

    // Scheduled Tweet Queue Events
    this.scheduledTweetQueue.on('completed', (job, result) => {
      logger.info(`Scheduled tweet job ${job.id} completed:`, result);
    });

    this.scheduledTweetQueue.on('failed', (job, err) => {
      logger.error(`Scheduled tweet job ${job.id} failed:`, err.message);
    });
  }

  // Add immediate tweet to queue
  async addTweetJob(tweetData, options = {}) {
    try {
      const job = await this.tweetQueue.add('process-tweet', tweetData, {
        priority: options.priority || 10,
        delay: options.delay || 0,
        ...options,
      });
      logger.info(`Added tweet job to queue: ${job.id}`);
      return job;
    } catch (error) {
      logger.error('Failed to add tweet job to queue:', error);
      throw error;
    }
  }

  // Add scheduled tweet to queue
  async addScheduledTweetJob(scheduleData, cronExpression) {
    try {
      // Structure the job data to match what processScheduledTweet expects
      const jobData = {
        scheduleData: scheduleData
      };
      
      const job = await this.scheduledTweetQueue.add('process-scheduled-tweet', jobData, {
        repeat: { cron: cronExpression },
        removeOnComplete: true,
        removeOnFail: true,
      });
      logger.info(`Added scheduled tweet job: ${job.id} with cron: ${cronExpression}`);
      return job;
    } catch (error) {
      logger.error('Failed to add scheduled tweet job:', error);
      throw error;
    }
  }

  // Get queue statistics
  async getQueueStats() {
    try {
      const [tweetWaiting, tweetActive, tweetCompleted, tweetFailed] = await Promise.all([
        this.tweetQueue.getWaiting(),
        this.tweetQueue.getActive(),
        this.tweetQueue.getCompleted(),
        this.tweetQueue.getFailed(),
      ]);

      const [scheduledWaiting, scheduledActive, scheduledCompleted] = await Promise.all([
        this.scheduledTweetQueue.getWaiting(),
        this.scheduledTweetQueue.getActive(),
        this.scheduledTweetQueue.getCompleted(),
      ]);

      return {
        tweets: {
          waiting: tweetWaiting.length,
          active: tweetActive.length,
          completed: tweetCompleted.length,
          failed: tweetFailed.length,
        },
        scheduled: {
          waiting: scheduledWaiting.length,
          active: scheduledActive.length,
          completed: scheduledCompleted.length,
        },
      };
    } catch (error) {
      logger.error('Failed to get queue stats:', error);
      return null;
    }
  }

  // Cancel scheduled tweet
  async cancelScheduledTweet(jobId) {
    try {
      const job = await this.scheduledTweetQueue.getJob(jobId);
      if (job) {
        await job.remove();
        logger.info(`Cancelled scheduled tweet job: ${jobId}`);
        return true;
      }
      return false;
    } catch (error) {
      logger.error(`Failed to cancel scheduled tweet ${jobId}:`, error);
      throw error;
    }
  }

  // Smart rate limit management
  async canProcessTweet() {
    try {
      const dailyCount = await this.redisClient.get('daily_tweet_count') || 0;
      const remaining = config.rateLimits.dailyTweetLimit - parseInt(dailyCount);
      return remaining > 0;
    } catch (error) {
      logger.error('Failed to check tweet rate limit:', error);
      return false;
    }
  }

  async incrementDailyCount() {
    try {
      const today = new Date().toDateString();
      const key = `daily_tweet_count:${today}`;
      const count = await this.redisClient.incr(key);
      await this.redisClient.expire(key, 86400); // Expire in 24 hours
      return count;
    } catch (error) {
      logger.error('Failed to increment daily count:', error);
      throw error;
    }
  }

  async getDailyCount() {
    try {
      const today = new Date().toDateString();
      const key = `daily_tweet_count:${today}`;
      const count = await this.redisClient.get(key) || 0;
      return parseInt(count);
    } catch (error) {
      logger.error('Failed to get daily count:', error);
      return 0;
    }
  }

  // Cleanup method
  async close() {
    await this.tweetQueue.close();
    await this.scheduledTweetQueue.close();
    await this.dailyQuotaResetQueue.close();
    await this.redisClient.quit();
  }
}

module.exports = new QueueService();