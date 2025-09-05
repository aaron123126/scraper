import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import fs from 'fs/promises';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const config = {
  port: process.env.PORT || 3000,
  host: process.env.HOST || 'localhost',
  archivesDir: process.env.ARCHIVES_DIR || join(__dirname, '../archives'),
  tempDir: process.env.TEMP_DIR || join(__dirname, '../temp'),
  cacheDir: process.env.CACHE_DIR || join(__dirname, '../cache'),
  maxCacheSize: parseInt(process.env.MAX_CACHE_SIZE || 1073741824), // 1GB
  cacheTTL: parseInt(process.env.CACHE_TTL || 3600), // 1 hour
  maxUploadSize: parseInt(process.env.MAX_UPLOAD_SIZE || 5368709120), // 5GB
  rateLimitWindow: parseInt(process.env.RATE_LIMIT_WINDOW || 15),
  rateLimitMax: parseInt(process.env.RATE_LIMIT_MAX || 100),
  logLevel: process.env.LOG_LEVEL || 'info'
};

// Ensure directories exist
const ensureDirectories = async () => {
  const dirs = [config.archivesDir, config.tempDir, config.cacheDir];
  for (const dir of dirs) {
    await fs.mkdir(dir, { recursive: true });
  }
};

ensureDirectories().catch(console.error);

export default config;