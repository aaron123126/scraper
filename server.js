import dotenv from 'dotenv';
import { createServer } from 'http';
import { Server as SocketIOServer } from 'socket.io';
import app from './src/app.js';
import logger from './src/utils/logger.js';
import config from './src/config.js';

dotenv.config();

const server = createServer(app);
const io = new SocketIOServer(server, {
  cors: {
    origin: process.env.NODE_ENV === 'production' ? false : '*',
    methods: ['GET', 'POST']
  }
});

// Attach socket.io to app for use in routes
app.set('io', io);

// Socket.io connection handling
io.on('connection', (socket) => {
  logger.info(`Client connected: ${socket.id}`);
  
  socket.on('disconnect', () => {
    logger.info(`Client disconnected: ${socket.id}`);
  });
});

// Start server
const PORT = config.port;
const HOST = config.host;

server.listen(PORT, HOST, () => {
  logger.info(`🚀 Web Archive Server running at http://${HOST}:${PORT}`);
  logger.info(`📁 Archives directory: ${config.archivesDir}`);
  logger.info(`💾 Cache directory: ${config.cacheDir}`);
  logger.info(`🌐 Environment: ${process.env.NODE_ENV || 'development'}`);
});

// Graceful shutdown
process.on('SIGTERM', () => {
  logger.info('SIGTERM signal received: closing HTTP server');
  server.close(() => {
    logger.info('HTTP server closed');
    process.exit(0);
  });
});