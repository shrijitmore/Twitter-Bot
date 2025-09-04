const logger = require('../utils/logger');
const config = require('../config');

class DemoService {
  constructor() {
    this.demoData = {
      tweets: [
        {
          id: 'demo-1',
          text: 'Just launched our new AI-powered Twitter bot! 🚀 #AI #Twitter #Automation',
          schedule_type: 'daily',
          cron_time: '0 9 * * *',
          status: 'scheduled',
          created_at: new Date().toISOString(),
        },
        {
          id: 'demo-2',
          text: 'Sharing insights about the latest tech trends...',
          schedule_type: 'weekly',
          cron_time: '0 10 * * 1',
          status: 'scheduled',
          created_at: new Date().toISOString(),
        },
        {
          id: 'demo-3',
          text: 'Productivity tip: Use AI to automate your social media presence!',
          schedule_type: 'hourly',
          cron_time: '0 * * * *',
          status: 'sent',
          created_at: new Date().toISOString(),
        }
      ],
      stats: {
        todayTweets: 3,
        totalTweets: 47,
        scheduledTweets: 2,
      },
      rateLimitStatus: {
        success: true,
        limit: 17,
        remaining: 14,
        reset: Date.now() + 12 * 60 * 60 * 1000, // 12 hours from now
        resetTime: new Date(Date.now() + 12 * 60 * 60 * 1000).toISOString(),
        timeUntilReset: 12 * 60 * 60 * 1000,
        status: 'OK',
        message: 'You have 14 tweets remaining today.',
      }
    };
  }

  isDemoMode() {
    return config.isDemoMode;
  }

  getDemoTweets() {
    return this.demoData.tweets;
  }

  getDemoStats() {
    return this.demoData.stats;
  }

  getDemoRateLimitStatus() {
    return this.demoData.rateLimitStatus;
  }

  async generateDemoAITweet(prompt, options = {}) {
    // Simulate AI generation delay
    await new Promise(resolve => setTimeout(resolve, 1500));
    
    const sampleTweets = [
      `🚀 ${prompt} - The future is here and it's incredible! #Tech #Innovation`,
      `💡 Quick insight about ${prompt}: It's revolutionizing how we work and think! #AI #Future`,
      `🔥 Hot take on ${prompt}: This could be the game-changer we've been waiting for! #TechTrends`,
      `⚡ ${prompt} is transforming industries faster than ever. What's your take? #Innovation #Tech`,
      `🌟 Amazing developments in ${prompt}! The possibilities are endless. #Future #Technology`
    ];

    const randomTweet = sampleTweets[Math.floor(Math.random() * sampleTweets.length)];
    
    return {
      text: randomTweet.slice(0, 280),
      includeImage: options.includeImage || false,
      generatedAt: new Date().toISOString(),
    };
  }

  getDemoSuggestions() {
    return [
      'Latest developments in AI and machine learning',
      'Remote work productivity tips for 2025',
      'Sustainable technology innovations',
      'The future of social media automation',
      'Cybersecurity best practices for businesses'
    ];
  }

  showConfigurationMessage() {
    logger.warn('🚨 DEMO MODE: Application is running in demo mode.');
    logger.warn('To enable full functionality, please configure the following environment variables:');
    logger.warn('- SUPABASE_URL: Your Supabase project URL');
    logger.warn('- SUPABASE_KEY: Your Supabase API key');
    logger.warn('- TWITTER_API_KEY: Your Twitter API key');
    logger.warn('- TWITTER_API_SECRET: Your Twitter API secret');
    logger.warn('- TWITTER_ACCESS_TOKEN: Your Twitter access token');
    logger.warn('- TWITTER_ACCESS_SECRET: Your Twitter access secret');
    logger.warn('- GEMINI_API_KEY: Your Google Gemini API key');
  }
}

module.exports = new DemoService();