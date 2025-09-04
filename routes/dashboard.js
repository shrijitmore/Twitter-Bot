const express = require('express');
const router = express.Router();
const databaseService = require('../services/DatabaseService');
const queueService = require('../services/QueueService');
const twitterService = require('../services/TwitterService');
const logger = require('../utils/logger');

// Dashboard API endpoints
router.get('/stats', async (req, res) => {
  try {
    const [tweetStats, queueStats, rateLimitStatus] = await Promise.all([
      databaseService.getTweetStats(),
      queueService.getQueueStats(),
      twitterService.getRateLimitStatus(),
    ]);

    res.json({
      success: true,
      data: {
        tweets: tweetStats,
        queues: queueStats,
        rateLimit: rateLimitStatus,
      },
    });
  } catch (error) {
    logger.error('Failed to get dashboard stats:', error.message);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

// Tweet history endpoint
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

// Queue management endpoints
router.get('/queue/stats', async (req, res) => {
  try {
    const stats = await queueService.getQueueStats();
    res.json({ success: true, data: stats });
  } catch (error) {
    logger.error('Failed to get queue stats:', error.message);
    res.status(500).json({ success: false, error: error.message });
  }
});

router.post('/queue/clear', async (req, res) => {
  try {
    // Only allow clearing failed jobs for safety
    const { queueType } = req.body;
    
    if (queueType === 'failed') {
      await queueService.tweetQueue.clean(0, 'failed');
      await queueService.scheduledTweetQueue.clean(0, 'failed');
      
      logger.info('Failed jobs cleared from queues');
      res.json({ success: true, message: 'Failed jobs cleared successfully' });
    } else {
      res.status(400).json({ success: false, error: 'Invalid queue type' });
    }
  } catch (error) {
    logger.error('Failed to clear queue:', error.message);
    res.status(500).json({ success: false, error: error.message });
  }
});

// System maintenance endpoints
router.post('/maintenance/cleanup', async (req, res) => {
  try {
    await databaseService.cleanup();
    res.json({ success: true, message: 'Database cleanup completed' });
  } catch (error) {
    logger.error('Maintenance cleanup failed:', error.message);
    res.status(500).json({ success: false, error: error.message });
  }
});

module.exports = router;