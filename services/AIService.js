const { GoogleGenerativeAI } = require('@google/generative-ai');
const config = require('../config');
const logger = require('../utils/logger');
const axios = require('axios');

class AIService {
  constructor() {
    this.ai = new GoogleGenerativeAI(config.ai.geminiApiKey);
    
    // Store previous tweets to ensure variety
    this.previousTweets = [];
    this.maxHistory = 20;
  }

  async generateTweet(prompt, options = {}) {
    try {
      const { includeImage = false, tone = 'engaging', maxLength = 280 } = options;
      
      // Build context from previous tweets to avoid repetition
      const previousContext = this.previousTweets.length > 0 
        ? `Previous tweets to avoid repeating (DO NOT copy these): ${this.previousTweets.slice(0, 10).join(' | ')}` 
        : '';

      const enhancedPrompt = this.buildPrompt(prompt, tone, maxLength, previousContext);
      
      const model = this.ai.getGenerativeModel({ model: 'gemini-1.5-flash' });
      const response = await model.generateContent(enhancedPrompt);
      
      const tweetText = this.cleanTweetText(response.response.text(), maxLength);
      
      // Store in history to avoid repetition
      this.addToHistory(tweetText);
      
      logger.info('AI tweet generated successfully:', {
        prompt: prompt.substring(0, 50) + '...',
        length: tweetText.length,
      });

      return {
        text: tweetText,
        includeImage,
        generatedAt: new Date().toISOString(),
      };
    } catch (error) {
      logger.error('Failed to generate AI tweet:', error.message);
      
      // Provide more specific error messages
      if (error.message.includes('API_KEY_INVALID') || error.message.includes('demo')) {
        throw new Error('AI service not configured. Please set GEMINI_API_KEY in your .env file.');
      } else if (error.message.includes('QUOTA_EXCEEDED')) {
        throw new Error('AI service quota exceeded. Please check your Gemini API usage limits.');
      } else if (error.message.includes('SAFETY')) {
        throw new Error('AI service blocked content due to safety filters. Please try a different prompt.');
      } else {
        throw new Error(`AI tweet generation failed: ${error.message}`);
      }
    }
  }

  buildPrompt(prompt, tone, maxLength, previousContext) {
    const toneInstructions = {
      engaging: 'Be engaging, conversational, and use emojis where appropriate.',
      professional: 'Use professional language, be informative and authoritative.',
      casual: 'Be casual, friendly, and relatable.',
      humorous: 'Add humor and wit while staying relevant.',
      informative: 'Focus on sharing valuable information and insights.',
    };

    return `Write a ${tone} Twitter post about: ${prompt}

Instructions:
- Keep it under ${maxLength} characters
- Be original, creative, and authentic
- ${toneInstructions[tone] || toneInstructions.engaging}
- Make it shareable and likely to get engagement
- Include relevant hashtags if appropriate (max 2-3)
- Use line breaks for readability if needed

${previousContext}

Important: Return ONLY the tweet text, no quotes, no explanations, no additional text.`;
  }

  cleanTweetText(text, maxLength) {
    // Remove quotes and clean up the text
    let cleaned = text
      .replace(/^["']|["']$/g, '') // Remove leading/trailing quotes
      .replace(/^Tweet:\s*/i, '') // Remove "Tweet:" prefix
      .trim();

    // Ensure it fits within character limit
    if (cleaned.length > maxLength) {
      cleaned = cleaned.substring(0, maxLength - 3) + '...';
    }

    return cleaned;
  }

  addToHistory(tweetText) {
    this.previousTweets.unshift(tweetText);
    if (this.previousTweets.length > this.maxHistory) {
      this.previousTweets = this.previousTweets.slice(0, this.maxHistory);
    }
  }

  async generateImagePrompt(tweetText) {
    try {
      const model = this.ai.getGenerativeModel({ model: 'gemini-1.5-flash' });
      const response = await model.generateContent(`Based on this tweet: "${tweetText}"

Create a short, descriptive image prompt (under 100 characters) that would create a relevant, engaging image for this tweet. Focus on:
- Visual elements that complement the tweet
- Professional, clean aesthetic
- Avoid text in the image
- Make it visually appealing for social media

Return only the image prompt, no explanations.`);
      
      const imagePrompt = response.response.text().replace(/^["']|["']$/g, '').trim();
      
      logger.info('Image prompt generated:', imagePrompt);
      return imagePrompt;
    } catch (error) {
      logger.error('Failed to generate image prompt:', error.message);
      return 'Professional, minimalist design with modern colors';
    }
  }

  async generateMultipleTweets(prompt, count = 3, options = {}) {
    try {
      const tweets = [];
      const { tone = 'engaging', variety = true } = options;
      
      for (let i = 0; i < count; i++) {
        const currentTone = variety ? this.getRandomTone() : tone;
        const variation = variety ? this.addPromptVariation(prompt, i) : prompt;
        
        const tweet = await this.generateTweet(variation, { ...options, tone: currentTone });
        tweets.push({
          ...tweet,
          tone: currentTone,
          variation: i + 1,
        });
        
        // Small delay to ensure variety
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
      
      return tweets;
    } catch (error) {
      logger.error('Failed to generate multiple tweets:', error.message);
      throw error;
    }
  }

  getRandomTone() {
    const tones = ['engaging', 'professional', 'casual', 'informative'];
    return tones[Math.floor(Math.random() * tones.length)];
  }

  addPromptVariation(basePrompt, index) {
    const variations = [
      basePrompt,
      `Share insights about: ${basePrompt}`,
      `What are your thoughts on: ${basePrompt}`,
      `Let's discuss: ${basePrompt}`,
      `Breaking down: ${basePrompt}`,
    ];
    
    return variations[index % variations.length];
  }

  // Get tweet suggestions based on trending topics
  async getTweetSuggestions(category = 'tech') {
    const suggestions = {
      tech: [
        'Latest AI developments and their impact on productivity',
        'Remote work tips for developers',
        'Open source projects worth contributing to',
        'Programming languages trending in 2025',
        'Best practices for code reviews',
      ],
      business: [
        'Startup lessons learned this week',
        'Customer feedback and product improvements',
        'Team building strategies that work',
        'Marketing insights from recent campaigns',
        'Industry trends and predictions',
      ],
      general: [
        'Daily motivation and productivity tips',
        'Interesting facts you learned today',
        'Book recommendations and key takeaways',
        'Weekend project ideas',
        'Personal growth insights',
      ],
    };

    return suggestions[category] || suggestions.general;
  }

  // Clear history (useful for testing or reset)
  clearHistory() {
    this.previousTweets = [];
    logger.info('AI tweet history cleared');
  }
}

module.exports = new AIService();