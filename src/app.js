import express from 'express';
import { engine } from 'express-handlebars';
import compression from 'compression';
import helmet from 'helmet';
import cors from 'cors';
import morgan from 'morgan';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

import apiRoutes from './routes/api.js';
import viewerRoutes from './routes/viewer.js';
import errorHandler from './middleware/errorHandler.js';
import security from './middleware/security.js';
import logger from './utils/logger.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const app = express();

// View engine setup
app.engine('hbs', engine({
  extname: '.hbs',
  defaultLayout: 'main',
  layoutsDir: join(__dirname, '../views/layouts'),
  partialsDir: join(__dirname, '../views/partials'),
  helpers: {
    json: (context) => JSON.stringify(context),
    formatBytes: (bytes) => {
      if (bytes === 0) return '0 Bytes';
      const k = 1024;
      const sizes = ['Bytes', 'KB', 'MB', 'GB'];
      const i = Math.floor(Math.log(bytes) / Math.log(k));
      return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    },
    formatDate: (date) => new Date(date).toLocaleString()
  }
}));
app.set('view engine', 'hbs');
app.set('views', join(__dirname, '../views'));

// Middleware
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      styleSrc: ["'self'", "'unsafe-inline'", "https://cdn.jsdelivr.net"],
      scriptSrc: ["'self'", "'unsafe-inline'", "https://cdn.jsdelivr.net", "https://cdn.socket.io"],
      fontSrc: ["'self'", "https://fonts.gstatic.com"],
      imgSrc: ["'self'", "data:", "https:"],
      connectSrc: ["'self'", "ws:", "wss:"]
    }
  }
}));
app.use(cors());
app.use(compression());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));
app.use(morgan('combined', { stream: { write: message => logger.info(message.trim()) } }));

// Static files
app.use(express.static(join(__dirname, '../public')));
app.use('/archives', express.static(join(__dirname, '../cache')));

// Security middleware
app.use(security);

// Routes
app.use('/api', apiRoutes);
app.use('/', viewerRoutes);

// Error handling
app.use(errorHandler);

export default app;