import logger from '../utils/logger.js';

export default function errorHandler(err, req, res, next) {
  logger.error('Error:', err);

  const status = err.status || 500;
  const message = err.message || 'Internal Server Error';

  if (req.accepts('json')) {
    res.status(status).json({
      success: false,
      error: message,
      ...(process.env.NODE_ENV === 'development' && { stack: err.stack })
    });
  } else {
    res.status(status).render('error', {
      title: 'Error',
      message,
      status
    });
  }
}