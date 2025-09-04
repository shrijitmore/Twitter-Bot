const supabase = require('../supabaseClient');
const logger = require('../utils/logger');
const { v4: uuidv4 } = require('uuid');

class DatabaseService {
  constructor() {
    this.tables = {
      scheduledTweets: 'scheduled_tweets',
      tweetHistory: 'tweet_history',
      rateLimits: 'rate_limits',
    };
  }

  // Scheduled Tweets Operations
  async createScheduledTweet(tweetData) {
    try {
      const data = {
        id: uuidv4(),
        ...tweetData,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      };

      const { data: result, error } = await supabase
        .from(this.tables.scheduledTweets)
        .insert([data])
        .select()
        .single();

      if (error) throw error;

      logger.info('Scheduled tweet created:', result.id);
      return result;
    } catch (error) {
      logger.error('Failed to create scheduled tweet:', error.message);
      throw error;
    }
  }

  async getScheduledTweets(status = null) {
    try {
      let query = supabase
        .from(this.tables.scheduledTweets)
        .select('*')
        .order('created_at', { ascending: false });

      if (status) {
        query = query.eq('status', status);
      }

      const { data, error } = await query;
      if (error) throw error;

      return data;
    } catch (error) {
      logger.error('Failed to get scheduled tweets:', error.message);
      throw error;
    }
  }

  async updateScheduledTweet(id, updates) {
    try {
      if (!id) {
        throw new Error('Missing required parameter: id');
      }

      const updateData = {
        ...updates,
        updated_at: new Date().toISOString(),
      };

      logger.debug('Updating scheduled tweet:', { id, updateData });

      const { data, error } = await supabase
        .from(this.tables.scheduledTweets)
        .update(updateData)
        .eq('id', id)
        .select()
        .single();

      if (error) {
        logger.error('Supabase update error:', {
          code: error.code,
          details: error.details,
          hint: error.hint,
          message: error.message
        });
        throw error;
      }

      if (!data) {
        throw new Error(`No scheduled tweet found with id: ${id}`);
      }

      logger.info('Scheduled tweet updated successfully:', id);
      return data;
    } catch (error) {
      logger.error('Failed to update scheduled tweet:', {
        id,
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  async deleteScheduledTweet(id) {
    try {
      const { error } = await supabase
        .from(this.tables.scheduledTweets)
        .delete()
        .eq('id', id);

      if (error) throw error;

      logger.info('Scheduled tweet deleted:', id);
      return true;
    } catch (error) {
      logger.error('Failed to delete scheduled tweet:', error.message);
      throw error;
    }
  }

  // Tweet History Operations
  async saveTweetHistory(historyData) {
    try {
      const data = {
        id: uuidv4(),
        ...historyData,
        posted_at: new Date().toISOString(),
      };

      const { data: result, error } = await supabase
        .from(this.tables.tweetHistory)
        .insert([data])
        .select()
        .single();

      if (error) throw error;

      logger.info('Tweet history saved:', result.id);
      return result;
    } catch (error) {
      logger.error('Failed to save tweet history:', error.message);
      throw error;
    }
  }

  async getTweetHistory(limit = 50, offset = 0) {
    try {
      const { data, error } = await supabase
        .from(this.tables.tweetHistory)
        .select('*')
        .order('posted_at', { ascending: false })
        .range(offset, offset + limit - 1);

      if (error) throw error;

      return data;
    } catch (error) {
      logger.error('Failed to get tweet history:', error.message);
      throw error;
    }
  }

  async getTweetStats() {
    try {
      // Get today's tweet count
      const today = new Date().toDateString();
      const startOfDay = new Date(today).toISOString();
      
      const { data: todayTweets, error: todayError } = await supabase
        .from(this.tables.tweetHistory)
        .select('id')
        .gte('posted_at', startOfDay);

      if (todayError) throw todayError;

      // Get total tweet count
      const { count: totalTweets, error: totalError } = await supabase
        .from(this.tables.tweetHistory)
        .select('*', { count: 'exact', head: true });

      if (totalError) throw totalError;

      // Get scheduled tweets count
      const { count: scheduledCount, error: scheduledError } = await supabase
        .from(this.tables.scheduledTweets)
        .select('*', { count: 'exact', head: true })
        .eq('status', 'scheduled');

      if (scheduledError) throw scheduledError;

      return {
        todayTweets: todayTweets.length,
        totalTweets: totalTweets || 0,
        scheduledTweets: scheduledCount || 0,
      };
    } catch (error) {
      logger.error('Failed to get tweet stats:', error.message);
      return {
        todayTweets: 0,
        totalTweets: 0,
        scheduledTweets: 0,
      };
    }
  }

  // Rate Limits Operations
  async saveRateLimitStatus(rateLimitData) {
    try {
      const data = {
        id: uuidv4(),
        ...rateLimitData,
        recorded_at: new Date().toISOString(),
      };

      const { data: result, error } = await supabase
        .from(this.tables.rateLimits)
        .insert([data])
        .select()
        .single();

      if (error) throw error;

      return result;
    } catch (error) {
      logger.error('Failed to save rate limit status:', error.message);
      throw error;
    }
  }

  // Utility methods
  async cleanup() {
    try {
      // Clean up old completed/failed scheduled tweets (older than 7 days)
      const sevenDaysAgo = new Date();
      sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);

      const { error: cleanupError } = await supabase
        .from(this.tables.scheduledTweets)
        .delete()
        .in('status', ['completed', 'failed', 'cancelled'])
        .lt('updated_at', sevenDaysAgo.toISOString());

      if (cleanupError) {
        logger.warn('Cleanup warning:', cleanupError.message);
      } else {
        logger.info('Database cleanup completed');
      }

      // Clean up old tweet history (keep last 1000 records)
      const { data: oldRecords, error: selectError } = await supabase
        .from(this.tables.tweetHistory)
        .select('id')
        .order('posted_at', { ascending: false })
        .range(1000, 2000); // Get records beyond the 1000 most recent

      if (selectError) {
        logger.warn('Cleanup select error:', selectError.message);
        return;
      }

      if (oldRecords && oldRecords.length > 0) {
        const oldIds = oldRecords.map(record => record.id);
        const { error: deleteError } = await supabase
          .from(this.tables.tweetHistory)
          .delete()
          .in('id', oldIds);

        if (deleteError) {
          logger.warn('History cleanup error:', deleteError.message);
        } else {
          logger.info(`Cleaned up ${oldIds.length} old tweet history records`);
        }
      }
    } catch (error) {
      logger.error('Database cleanup failed:', error.message);
    }
  }

  // Health check
  async healthCheck() {
    try {
      const { error } = await supabase
        .from(this.tables.tweetHistory)
        .select('id')
        .limit(1);

      if (error) throw error;

      return { status: 'healthy', timestamp: new Date().toISOString() };
    } catch (error) {
      logger.error('Database health check failed:', error.message);
      return { status: 'unhealthy', error: error.message, timestamp: new Date().toISOString() };
    }
  }
}

module.exports = new DatabaseService();