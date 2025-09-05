import rateLimit from 'express-rate-limit';
import config from '../config.js';

const limiter = rateLimit({
  windowMs: config.rateLimitWindow * 60 * 1000,
  max: config.rateLimitMax,
  message: 'Too many requests, please try again later.',
  standardHeaders: true,
  legacyHeaders: false
});

export default limiter;