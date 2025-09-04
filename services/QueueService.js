// services/QueueService.js
const Queue = require("bull");
const Redis = require("ioredis");
const config = require("../config");
const logger = require("../utils/logger");

class QueueService {
  constructor() {
    // ---- Shared Redis Connection ----
    if (config.redis.url) {
      this.redisConnection = new Redis(config.redis.url, {
        maxRetriesPerRequest: null,
        tls: config.redis.url.startsWith("rediss://") ? {} : undefined,
      });
    } else {
      this.redisConnection = new Redis({
        host: config.redis.host,
        port: config.redis.port,
        password: config.redis.password || undefined,
        maxRetriesPerRequest: null,
      });
    }

    this.redisConnection.on("error", (err) => {
      logger.error("Redis Client Error:", err);
    });

    this.redisConnection.on("connect", () => {
      logger.info("✅ Connected to Redis");
    });

    // ---- Bull Queues using shared connection ----
    this.tweetQueue = new Queue("tweet processing", {
      createClient: () => this.redisConnection,
      defaultJobOptions: {
        removeOnComplete: 100,
        removeOnFail: 50,
        attempts: 3,
        backoff: { type: "exponential", delay: 2000 },
      },
    });

    this.scheduledTweetQueue = new Queue("scheduled tweets", {
      createClient: () => this.redisConnection,
      defaultJobOptions: {
        removeOnComplete: 50,
        removeOnFail: 25,
        attempts: 5,
        backoff: { type: "exponential", delay: 5000 },
      },
    });

    this.dailyQuotaResetQueue = new Queue("daily quota reset", {
      createClient: () => this.redisConnection,
    });

    this.setupQueueEvents();
  }

  // ---- Job Adders ----
  async addTweetJob(tweetData) {
    try {
      const job = await this.tweetQueue.add(tweetData);
      logger.info("Tweet job added", { jobId: job.id });
      return job;
    } catch (err) {
      logger.error("Failed to add tweet job", err);
      throw err;
    }
  }

  async scheduleTweet(tweetData, scheduleTime) {
    try {
      const job = await this.scheduledTweetQueue.add(tweetData, {
        delay: Math.max(scheduleTime - Date.now(), 0),
      });
      logger.info("Scheduled tweet created", { jobId: job.id });
      return job;
    } catch (err) {
      logger.error("Failed to add scheduled tweet job", err);
      throw err;
    }
  }

  async scheduleQuotaReset(resetTime) {
    try {
      await this.dailyQuotaResetQueue.empty(); // clear old reset jobs
      const job = await this.dailyQuotaResetQueue.add(
        {},
        {
          delay: Math.max(resetTime - Date.now(), 0),
          attempts: 3,
          removeOnComplete: true,
        }
      );
      logger.info("Scheduled quota reset", { jobId: job.id });
      return job;
    } catch (err) {
      logger.error("Failed to schedule quota reset job", err);
      throw err;
    }
  }

  // ---- Queue Event Logging ----
  setupQueueEvents() {
    const queues = [
      { name: "tweet", queue: this.tweetQueue },
      { name: "scheduled", queue: this.scheduledTweetQueue },
      { name: "quota reset", queue: this.dailyQuotaResetQueue },
    ];

    queues.forEach(({ name, queue }) => {
      queue.on("completed", (job) => {
        logger.info(`${name} job completed`, { jobId: job.id });
      });
      queue.on("failed", (job, err) => {
        logger.error(`${name} job failed`, { jobId: job?.id, error: err });
      });
    });
  }
}

module.exports = new QueueService();
