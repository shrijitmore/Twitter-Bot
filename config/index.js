require('dotenv').config();

const config = {
  port: process.env.PORT || 8080,
  
  // Redis Configuration
  redis: process.env.REDIS_URL
  ? { url: process.env.REDIS_URL } // Railway (single URL)
  : {                              // Local dev
      host: process.env.REDIS_HOST || 'localhost',
      port: process.env.REDIS_PORT || 6379,
      password: process.env.REDIS_PASSWORD || null,
    },
  
  // Supabase Configuration
  supabase: {
    url: process.env.SUPABASE_URL || 'https://demo.supabase.co',
    key: process.env.SUPABASE_KEY || 'demo-key',
  },
  
  // Twitter API Configuration
  twitter: {
    apiKey: process.env.TWITTER_API_KEY || 'demo-key',
    apiSecret: process.env.TWITTER_API_SECRET || 'demo-secret',
    accessToken: process.env.TWITTER_ACCESS_TOKEN || 'demo-token',
    accessSecret: process.env.TWITTER_ACCESS_SECRET || 'demo-access-secret',
  },
  
  // AI Configuration
  ai: {
    geminiApiKey: process.env.GEMINI_API_KEY || 'demo-gemini-key',
  },
  
  // Rate Limiting Configuration
  rateLimits: {
    dailyTweetLimit: 17, // Twitter free tier limit
    apiRequestsPerMinute: 60,
    schedulingDistributionHours: 24,
  },
  
  // File Upload Configuration
  uploads: {
    maxFileSize: 5 * 1024 * 1024, // 5MB
    allowedMimes: ['image/jpeg', 'image/png', 'image/gif', 'image/webp'],
    uploadDir: 'uploads/',
  },
  
  // Logging Configuration
  logging: {
    level: process.env.LOG_LEVEL || 'info',
    file: process.env.LOG_FILE || 'app.log',
  },

  // Demo mode (when API keys are not configured)
  isDemoMode: !process.env.SUPABASE_URL || 
              !process.env.TWITTER_API_KEY || 
              !process.env.GEMINI_API_KEY ||
              process.env.SUPABASE_URL.includes('your_') ||
              process.env.TWITTER_API_KEY.includes('your_') ||
              process.env.GEMINI_API_KEY.includes('your_'),
};

module.exports = config;