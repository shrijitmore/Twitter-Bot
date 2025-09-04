const express = require('express');
const router = express.Router();
const multer = require('multer');
const path = require('path');
const fs = require('fs');

// Import services and utilities
const { validate, schemas } = require('../utils/validation');
const logger = require('../utils/logger');
const config = require('../config');
const demoService = require('../services/DemoService');

// Only import real services if not in demo mode
let twitterService, aiService, databaseService, queueService;
if (!config.isDemoMode) {
  twitterService = require('../services/TwitterService');
  aiService = require('../services/AIService');
  databaseService = require('../services/DatabaseService');
  queueService = require('../services/QueueService');
}

// Configure multer for file uploads
const upload = multer({
  dest: config.uploads.uploadDir,
  limits: {
    fileSize: config.uploads.maxFileSize,
  },
  fileFilter: (req, file, cb) => {
    if (config.uploads.allowedMimes.includes(file.mimetype)) {
      cb(null, true);
    } else {
      cb(new Error('Invalid file type. Only images are allowed.'));
    }
  },
});

// ---- Rate Limit Status ----
router.get('/rate-status', async (req, res) => {
  try {
    if (config.isDemoMode) {
      res.json(demoService.getDemoRateLimitStatus());
      return;
    }
    
    const status = await twitterService.getRateLimitStatus();
    res.json(status);
  } catch (error) {
    logger.error('Rate limit check failed:', error.message);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

// ---- AI Tweet Generation (One-time) ----
router.post('/ai-generate', async (req, res) => {
  try {
    const validatedData = validate(schemas.aiGenerate, req.body);
    const { prompt, includeImage, imagePrompt } = validatedData;

    let aiResult;
    if (config.isDemoMode) {
      aiResult = await demoService.generateDemoAITweet(prompt, {
        includeImage,
        tone: req.body.tone || 'engaging',
      });
    } else {
      // Generate AI tweet
      aiResult = await aiService.generateTweet(prompt, {
        includeImage,
        tone: req.body.tone || 'engaging',
      });
    }

    res.json({
      success: true,
      data: {
        text: aiResult.text,
        includeImage: aiResult.includeImage,
        generatedAt: aiResult.generatedAt,
        prompt,
        demoMode: config.isDemoMode,
      },
    });

  } catch (error) {
    logger.error('AI tweet generation failed:', error.message);
    res.status(400).json({
      success: false,
      error: error.message,
    });
  }
});

// ---- AI Multiple Tweet Suggestions ----
router.post('/ai-suggestions', async (req, res) => {
  try {
    const { prompt, count = 3, tone = 'engaging' } = req.body;
    
    if (!prompt || prompt.length < 10) {
      return res.status(400).json({
        success: false,
        error: 'Prompt must be at least 10 characters long',
      });
    }

    const suggestions = await aiService.generateMultipleTweets(prompt, count, {
      tone,
      variety: true,
    });

    res.json({
      success: true,
      data: suggestions,
    });

  } catch (error) {
    logger.error('AI suggestions generation failed:', error.message);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

// ---- Post Tweet Immediately ----
router.post('/post', upload.single('image'), async (req, res) => {
  try {
    const validatedData = validate(schemas.tweet, req.body);
    const { text, imagePrompt } = validatedData;

    if (config.isDemoMode) {
      // Clean up uploaded file if it exists
      if (req.file && fs.existsSync(req.file.path)) {
        fs.unlinkSync(req.file.path);
      }
      
      return res.render('success', { 
        message: '✅ Demo Mode: Tweet would be posted immediately in production mode! Configure API keys to enable real posting.',
      });
    }

    // Check rate limits
    const rateLimitStatus = await twitterService.getRateLimitStatus();
    if (rateLimitStatus.status !== 'OK') {
      return res.status(429).render('error', {
        message: `❌ ${rateLimitStatus.message}`,
      });
    }

    // Prepare tweet data
    const tweetData = {
      text,
      imagePrompt,
      imageFile: req.file ? req.file.path : null,
    };

    // Add to immediate tweet queue
    await queueService.addTweetJob({ tweetData });

    res.render('success', { 
      message: '✅ Tweet queued for posting!',
    });

  } catch (error) {
    logger.error('Tweet posting failed:', error.message);
    
    // Clean up uploaded file if it exists
    if (req.file && fs.existsSync(req.file.path)) {
      fs.unlinkSync(req.file.path);
    }

    res.render('error', { 
      message: `❌ ${error.message}`,
    });
  }
});

// ---- Schedule Tweet ----
router.post('/schedule', upload.single('image'), async (req, res) => {
  try {
    const validatedData = validate(schemas.schedule, req.body);
    const { 
      text, 
      scheduleType, 
      customPrompt, 
      imagePrompt, 
      includeImage, 
      customCron, 
      time 
    } = validatedData;

    // Clean up uploaded file if it exists
    if (req.file && fs.existsSync(req.file.path)) {
      fs.unlinkSync(req.file.path);
    }

    if (config.isDemoMode) {
      return res.render('success', { 
        message: '✅ Demo Mode: Tweet would be scheduled successfully in production mode! The scheduling system would use Redis-based queues to manage the 17 req/day limit efficiently. Configure API keys to enable real scheduling.',
      });
    }

    // Convert schedule type to cron expression
    let cronTime;
    switch (scheduleType) {
      case 'everyMinute':
        cronTime = '*/1 * * * *';
        break;
      case 'hourly':
        cronTime = '0 * * * *';
        break;
      case 'daily':
        const [hour, minute] = (time || '00:00').split(':');
        cronTime = `${minute} ${hour} * * *`;
        break;
      case 'weekly':
        const [wHour, wMinute] = (time || '00:00').split(':');
        cronTime = `${wMinute} ${wHour} * * 0`; // Sunday
        break;
      case 'custom':
        cronTime = customCron;
        break;
      default:
        throw new Error('Invalid schedule type');
    }

    // Process image if uploaded
    let imageUrl = null;
    if (req.file) {
      // For now, we'll use a placeholder URL
      imageUrl = 'https://picsum.photos/800/600';
    }

    // Create scheduled tweet record
    const scheduleData = {
      schedule_type: scheduleType,
      cron_time: cronTime,
      status: 'scheduled',
      custom_prompt: customPrompt || null,
      include_image: includeImage === 'true' || includeImage === true,
      image_url: imageUrl,
      image_prompt: imagePrompt || null,
      text: text || null,
    };

    const scheduledTweet = await databaseService.createScheduledTweet(scheduleData);

    // Add to scheduled tweet queue with properly structured data
    const jobData = {
      id: scheduledTweet.id,
      schedule_type: scheduleType,
      custom_prompt: customPrompt || null,
      include_image: includeImage === 'true' || includeImage === true,
      image_prompt: imagePrompt || null,
      text: text || null,
      status: 'scheduled'
    };
    
    await queueService.addScheduledTweetJob(jobData, cronTime);

    logger.info('Tweet scheduled successfully:', scheduledTweet.id);
    res.render('success', { 
      message: '✅ Tweet scheduled successfully!',
    });

  } catch (error) {
    logger.error('Tweet scheduling failed:', error.message);
    
    res.render('error', { 
      message: `❌ ${error.message}`,
    });
  }
});

// ---- View Scheduled Tweets ----
router.get('/scheduled', async (req, res) => {
  try {
    const scheduledTweets = await databaseService.getScheduledTweets();
    res.render('scheduled', { tweets: scheduledTweets });
  } catch (error) {
    logger.error('Failed to get scheduled tweets:', error.message);
    res.render('error', { message: `❌ ${error.message}` });
  }
});

// ---- Cancel Scheduled Tweet ----
router.post('/cancel/:id', async (req, res) => {
  try {
    const { id } = req.params;
    
    await databaseService.updateScheduledTweet(id, { 
      status: 'cancelled',
      cancelled_at: new Date().toISOString(),
    });

    // Try to cancel from queue as well
    // Note: This is a best effort - job might already be processed
    try {
      await queueService.cancelScheduledTweet(id);
    } catch (queueError) {
      logger.warn('Could not cancel job from queue:', queueError.message);
    }

    logger.info('Tweet cancelled:', id);
    res.render('success', { message: '✅ Tweet cancelled successfully' });
  } catch (error) {
    logger.error('Failed to cancel tweet:', error.message);
    res.render('error', { message: `❌ ${error.message}` });
  }
});

// ---- Tweet History API ----
router.get('/history', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 20;
    const offset = parseInt(req.query.offset) || 0;
    
    const history = await databaseService.getTweetHistory(limit, offset);
    
    res.json({
      success: true,
      data: history,
      pagination: {
        limit,
        offset,
        hasMore: history.length === limit,
      },
    });
  } catch (error) {
    logger.error('Failed to get tweet history:', error.message);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

// ---- AI Tweet Suggestions by Category ----
router.get('/ai-suggestions/:category', async (req, res) => {
  try {
    const { category } = req.params;
    
    let suggestions;
    if (config.isDemoMode) {
      suggestions = demoService.getDemoSuggestions();
    } else {
      suggestions = await aiService.getTweetSuggestions(category);
    }
    
    res.json({
      success: true,
      data: suggestions,
      category,
      demoMode: config.isDemoMode,
    });
  } catch (error) {
    logger.error('Failed to get AI suggestions:', error.message);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

module.exports = router;
