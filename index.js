require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const path = require('path');
const helmet = require('helmet');
const compression = require('compression');
const rateLimit = require('express-rate-limit');

// Import services and configuration
const config = require('./config');
const logger = require('./utils/logger');
const demoService = require('./services/DemoService');

// Only import real services if not in demo mode
let tweetProcessor, databaseService;
if (!config.isDemoMode) {
  tweetProcessor = require('./workers/tweetProcessor');
  databaseService = require('./services/DatabaseService');
} else {
  demoService.showConfigurationMessage();
}

const app = express();

// Security middleware
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      styleSrc: ["'self'", "'unsafe-inline'", "https://cdn.jsdelivr.net", "https://cdnjs.cloudflare.com"],
      scriptSrc: ["'self'", "'unsafe-inline'", "'unsafe-eval'"],
      scriptSrcAttr: ["'self'", "'unsafe-inline'"],
      imgSrc: ["'self'", "data:", "https:", "http:"],
      fontSrc: ["'self'", "https://cdnjs.cloudflare.com"],
      connectSrc: ["'self'", "https://api.twitter.com"],
    },
  },
}));

app.use(compression());


app.set('trust proxy', (ip) => {
  // Only trust Railway's proxy
  return ip === '::ffff:127.0.0.1' || // Localhost
         ip === '::1' ||              // IPv6 localhost
         ip === '127.0.0.1' ||        // IPv4 localhost
         ip.endsWith('.up.railway.app'); // Railway's domain
});

// Rate limiting
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: config.rateLimits.apiRequestsPerMinute, // limit each IP to 60 requests per windowMs
  message: 'Too many requests from this IP, please try again later.',
  standardHeaders: true,
  legacyHeaders: false,
});

app.use(limiter);

// Body parsing middleware
app.use(bodyParser.json({ limit: '10mb' }));
app.use(bodyParser.urlencoded({ extended: true, limit: '10mb' }));

// View engine setup
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

// Static files
app.use('/uploads', express.static(path.join(__dirname, 'uploads')));

// Routes
const tweetRoutes = require('./routes/tweet');
const dashboardRoutes = require('./routes/dashboard');

app.use('/tweet', tweetRoutes);
app.use('/dashboard', dashboardRoutes);

// Home route with enhanced dashboard
app.get('/', async (req, res) => {
  try {
    let scheduledTweets, tweetStats;
    
    if (config.isDemoMode) {
      scheduledTweets = demoService.getDemoTweets();
      tweetStats = demoService.getDemoStats();
    } else {
      [scheduledTweets, tweetStats] = await Promise.all([
        databaseService.getScheduledTweets(),
        databaseService.getTweetStats(),
      ]);
    }

    res.render('index', { 
      tweets: scheduledTweets,
      stats: tweetStats,
      isDemoMode: config.isDemoMode,
    });
  } catch (error) {
    logger.error('Error loading dashboard:', error.message);
    res.render('index', { 
      tweets: [],
      stats: { todayTweets: 0, totalTweets: 0, scheduledTweets: 0 },
      error: 'Failed to load dashboard data',
      isDemoMode: config.isDemoMode,
    });
  }
});

// Health check endpoint
app.get('/health', async (req, res) => {
  try {
    if (config.isDemoMode) {
      res.json({
        status: 'healthy',
        mode: 'demo',
        timestamp: new Date().toISOString(),
        message: 'Running in demo mode. Configure API keys to enable full functionality.',
        version: require('./package.json').version,
      });
      return;
    }

    const dbHealth = await databaseService.healthCheck();
    const queueService = require('./services/QueueService');
    const queueStats = await queueService.getQueueStats();

    res.json({
      status: 'healthy',
      mode: 'production',
      timestamp: new Date().toISOString(),
      database: dbHealth,
      queues: queueStats,
      version: require('./package.json').version,
    });
  } catch (error) {
    logger.error('Health check failed:', error.message);
    res.status(500).json({
      status: 'unhealthy',
      error: error.message,
      timestamp: new Date().toISOString(),
    });
  }
});

// Error handling middleware
app.use((error, req, res, next) => {
  logger.error('Unhandled error:', error);
  
  if (res.headersSent) {
    return next(error);
  }

  const status = error.status || 500;
  const message = process.env.NODE_ENV === 'production' 
    ? 'Internal Server Error' 
    : error.message;

  res.status(status).render('error', { 
    message: `❌ ${message}`,
    error: process.env.NODE_ENV === 'development' ? error : {},
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).render('error', { 
    message: '❌ Page not found',
    error: {},
  });
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  logger.info('SIGTERM received, shutting down gracefully...');
  await tweetProcessor.shutdown();
  process.exit(0);
});

process.on('SIGINT', async () => {
  logger.info('SIGINT received, shutting down gracefully...');
  await tweetProcessor.shutdown();
  process.exit(0);
});

// Start server
const PORT = config.port;
app.listen(PORT, () => {
  logger.info(`🚀 Twitter Bot server running on port ${PORT}`);
  logger.info(`Environment: ${process.env.NODE_ENV || 'development'}`);
  logger.info(`Mode: ${config.isDemoMode ? 'DEMO' : 'PRODUCTION'}`);
  logger.info(`Health check: http://localhost:${PORT}/health`);
  
  if (config.isDemoMode) {
    logger.info(`🎭 Demo Mode: Configure API keys in .env file to enable full functionality`);
  } else {
    // Run database cleanup on startup
    databaseService.cleanup().catch(err => {
      logger.warn('Initial database cleanup failed:', err.message);
    });
  }
});

module.exports = app;