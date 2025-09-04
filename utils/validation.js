const Joi = require('joi');

const schemas = {
  tweet: Joi.object({
    text: Joi.string().min(1).max(280).required(),
    imagePrompt: Joi.string().max(500).optional(),
  }),

  schedule: Joi.object({
    text: Joi.string().max(280).optional(),
    scheduleType: Joi.string().valid('everyMinute', 'hourly', 'daily', 'weekly', 'custom').required(),
    customPrompt: Joi.string().max(500).optional(),
    imagePrompt: Joi.string().max(500).optional(),
    includeImage: Joi.boolean().optional(),
    customCron: Joi.when('scheduleType', {
      is: 'custom',
      then: Joi.string().required().custom((value, helpers) => {
        // Simple validation that checks for 5 space-separated parts
        const parts = value.trim().split(/\s+/);
        if (parts.length !== 5) {
          return helpers.error('any.invalid');
        }
        return value;
      }, 'Cron Expression Validation')
      .message('Invalid cron format. Must have 5 parts: minute hour day month weekday'),
      otherwise: Joi.any().strip()
    }),
    time: Joi.when('scheduleType', {
      is: Joi.valid('daily', 'weekly'),
      then: Joi.string().pattern(/^([01]?[0-9]|2[0-3]):[0-5][0-9]$/).required(),
      otherwise: Joi.any().strip()
    })
  }),

  aiGenerate: Joi.object({
    prompt: Joi.string().min(10).max(500).required(),
    includeImage: Joi.boolean().optional(),
    imagePrompt: Joi.string().max(500).optional(),
    tone: Joi.string().valid('engaging', 'professional', 'casual', 'humorous', 'informative').default('engaging'),
  }),
};

const validate = (schema, data) => {
  const { error, value } = schema.validate(data);
  if (error) {
    throw new Error(`Validation error: ${error.details[0].message}`);
  }
  return value;
};

module.exports = {
  schemas,
  validate,
};