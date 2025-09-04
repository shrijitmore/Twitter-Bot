const queueService = require('../services/QueueService');
const twitterService = require('../services/TwitterService');
const aiService = require('../services/AIService');
const databaseService = require('../services/DatabaseService');
const logger = require('../utils/logger');
const fs = require('fs');
const path = require('path');
const axios = require('axios');

class TweetProcessor {
  constructor() {
    this.setupJobProcessors();
  }

  setupJobProcessors() {
    // Process immediate tweets
    queueService.tweetQueue.process('process-tweet', this.processTweet.bind(this));
    
    // Process scheduled tweets
    queueService.scheduledTweetQueue.process('process-scheduled-tweet', this.processScheduledTweet.bind(this));
  }

  async processTweet(job) {
    const { tweetData } = job.data;
    logger.info(`Processing immediate tweet job: ${job.id}`, tweetData);

    try {
      let mediaId = null;

      // Handle image if provided
      if (tweetData.imageFile) {
        mediaId = await twitterService.uploadMedia(tweetData.imageFile);
        // Clean up file after upload
        if (fs.existsSync(tweetData.imageFile)) {
          fs.unlinkSync(tweetData.imageFile);
        }
      } else if (tweetData.imageUrl) {
        const imagePath = await this.downloadImage(tweetData.imageUrl);
        mediaId = await twitterService.uploadMedia(imagePath);
        // Clean up downloaded file
        if (fs.existsSync(imagePath)) {
          fs.unlinkSync(imagePath);
        }
      }

      // Post the tweet
      const result = await twitterService.postTweet(tweetData.text, mediaId);

      // Save to history
      await databaseService.saveTweetHistory({
        text: tweetData.text,
        tweet_id: result.data.id,
        has_image: !!mediaId,
        image_prompt: tweetData.imagePrompt || null,
        type: 'immediate',
        status: 'success',
      });

      logger.info(`Tweet posted successfully: ${result.data.id}`);
      return { success: true, tweetId: result.data.id };

    } catch (error) {
      logger.error(`Tweet job ${job.id} failed:`, error.message);
      
      // Only save failed attempts to history if they're not due to API configuration issues
      // This prevents cluttering the database with failed attempts due to missing/invalid API keys
      const isConfigError = error.message.includes('demo') || 
                           error.message.includes('credentials') ||
                           error.message.includes('not configured') ||
                           error.message.includes('invalid') ||
                           error.message.includes('Unauthorized') ||
                           error.message.includes('Forbidden') ||
                           error.message.includes('Twitter API not configured') ||
                           error.message.includes('Twitter API credentials are invalid') ||
                           error.message.includes('Twitter API access forbidden') ||
                           error.message.includes('Twitter API rate limit exceeded') ||
                           error.message.includes('Request failed with code 429') || // Rate limit with demo keys
                           error.code === 401 ||
                           error.code === 403 ||
                           error.code === 429; // Rate limit exceeded (likely due to demo keys)
      
      if (!isConfigError) {
        await databaseService.saveTweetHistory({
          text: tweetData.text,
          has_image: !!tweetData.imageFile || !!tweetData.imageUrl,
          image_prompt: tweetData.imagePrompt || null,
          type: 'immediate',
          status: 'failed',
          error_message: error.message,
        });
      } else {
        logger.info('Skipping database save for configuration error:', error.message);
      }

      throw error;
    }
  }

  async processScheduledTweet(job) {
    logger.info(`Processing scheduled tweet job: ${job.id}`, { jobData: job.data });

    try {
      // Handle both direct data and nested scheduleData for backward compatibility
      const scheduleData = job.data.scheduleData || job.data;
      
      // Check if we have valid job data
      if (!scheduleData) {
        logger.error('Invalid job data for scheduled tweet:', job.data);
        throw new Error('Invalid job data - missing schedule data');
      }
      
      // Check if we can post (rate limits)
      const canPost = await queueService.canProcessTweet();
      if (!canPost) {
        logger.info(`Rate limit reached, deferring tweet job: ${job.id}`);
        // Re-queue for later (will be retried automatically by Bull)
        throw new Error('Daily rate limit reached');
      }

      let tweetText = scheduleData.text || scheduleData.custom_prompt;

      // Generate AI tweet if needed
      if (scheduleData.custom_prompt || !tweetText) {
        const prompt = scheduleData.custom_prompt || scheduleData.text || 'Share an interesting tech insight';
        const aiResult = await aiService.generateTweet(prompt, {
          includeImage: scheduleData.include_image,
          tone: scheduleData.tone || 'engaging',
        });
        tweetText = aiResult.text;
      }

      let mediaId = null;

      // Handle image generation/upload
      if (scheduleData.include_image) {
        if (scheduleData.image_url) {
          const imagePath = await this.downloadImage(scheduleData.image_url);
          mediaId = await twitterService.uploadMedia(imagePath);
          if (fs.existsSync(imagePath)) {
            fs.unlinkSync(imagePath);
          }
        } else if (scheduleData.image_prompt || scheduleData.custom_prompt) {
          // For now, use placeholder - in production you'd integrate with image generation API
          const placeholderUrl = 'https://picsum.photos/800/600';
          const imagePath = await this.downloadImage(placeholderUrl);
          mediaId = await twitterService.uploadMedia(imagePath);
          if (fs.existsSync(imagePath)) {
            fs.unlinkSync(imagePath);
          }
        }
      }

      // Post the tweet
      const result = await twitterService.postTweet(tweetText, mediaId);

      // Update scheduled tweet status
      if (scheduleData.id) {
        await databaseService.updateScheduledTweet(scheduleData.id, {
          status: 'sent',
          text: tweetText,
          tweet_id: result.data.id,
          sent_at: new Date().toISOString(),
        });
      }

      // Save to history
      await databaseService.saveTweetHistory({
        text: tweetText,
        tweet_id: result.data.id,
        has_image: !!mediaId,
        image_prompt: scheduleData.image_prompt || null,
        type: 'scheduled',
        schedule_type: scheduleData.schedule_type,
        status: 'success',
      });

      logger.info(`Scheduled tweet posted successfully: ${result.data.id}`);
      return { success: true, tweetId: result.data.id };

    } catch (error) {
      logger.error(`Scheduled tweet job ${job.id} failed:`, error.message);

      // Update scheduled tweet status to failed
      if (scheduleData.id) {
        await databaseService.updateScheduledTweet(scheduleData.id, {
          status: 'failed',
          error_message: error.message,
          failed_at: new Date().toISOString(),
        });
      }

      // Only save failed attempts to history if they're not due to API configuration issues
      const isConfigError = error.message.includes('demo') || 
                           error.message.includes('credentials') ||
                           error.message.includes('not configured') ||
                           error.message.includes('invalid') ||
                           error.message.includes('Unauthorized') ||
                           error.message.includes('Forbidden') ||
                           error.message.includes('Twitter API not configured') ||
                           error.message.includes('Twitter API credentials are invalid') ||
                           error.message.includes('Twitter API access forbidden') ||
                           error.message.includes('Twitter API rate limit exceeded') ||
                           error.message.includes('Request failed with code 429') || // Rate limit with demo keys
                           error.code === 401 ||
                           error.code === 403 ||
                           error.code === 429; // Rate limit exceeded (likely due to demo keys)
      
      if (!isConfigError) {
        await databaseService.saveTweetHistory({
          text: scheduleData.text || scheduleData.custom_prompt,
          has_image: scheduleData.include_image,
          image_prompt: scheduleData.image_prompt || null,
          type: 'scheduled',
          schedule_type: scheduleData.schedule_type,
          status: 'failed',
          error_message: error.message,
        });
      } else {
        logger.info('Skipping database save for configuration error:', error.message);
      }

      throw error;
    }
  }

  async downloadImage(url) {
    try {
      const response = await axios({
        url,
        method: 'GET',
        responseType: 'stream'
      });

      // Ensure uploads directory exists
      const uploadsDir = path.join(__dirname, '../uploads');
      if (!fs.existsSync(uploadsDir)) {
        fs.mkdirSync(uploadsDir, { recursive: true });
      }

      const fileName = `image-${Date.now()}-${Math.random().toString(36).substring(7)}.jpg`;
      const filePath = path.join(uploadsDir, fileName);

      const writer = fs.createWriteStream(filePath);
      response.data.pipe(writer);

      return new Promise((resolve, reject) => {
        writer.on('finish', () => resolve(filePath));
        writer.on('error', reject);
      });
    } catch (error) {
      logger.error('Failed to download image:', error.message);
      throw error;
    }
  }

  // Graceful shutdown
  async shutdown() {
    logger.info('Shutting down tweet processor...');
    await queueService.close();
  }
}

module.exports = new TweetProcessor();