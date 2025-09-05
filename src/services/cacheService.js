import NodeCache from 'node-cache';
import fs from 'fs/promises';
import { join } from 'path';
import crypto from 'crypto';
import logger from '../utils/logger.js';
import config from '../config.js';

class CacheService {
  constructor() {
    this.memCache = new NodeCache({ 
      stdTTL: config.cacheTTL,
      checkperiod: 600
    });
    this.cacheIndex = new Map();
    this.initializeCache();
  }

  async initializeCache() {
    try {
      // Load cache index
      const indexPath = join(config.cacheDir, 'index.json');
      const indexExists = await fs.access(indexPath).then(() => true).catch(() => false);
      
      if (indexExists) {
        const indexData = await fs.readFile(indexPath, 'utf-8');
        const index = JSON.parse(indexData);
        this.cacheIndex = new Map(Object.entries(index));
      }

      // Clean old cache entries
      await this.cleanOldCache();
    } catch (error) {
      logger.error('Failed to initialize cache:', error);
    }
  }

  async getCachePath(archiveId) {
    const cacheKey = `archive:${archiveId}`;
    
    // Check memory cache first
    const cached = this.memCache.get(cacheKey);
    if (cached) {
      logger.info(`Cache hit (memory): ${archiveId}`);
      return cached;
    }

    // Check disk cache
    if (this.cacheIndex.has(archiveId)) {
      const cachePath = this.cacheIndex.get(archiveId);
      const exists = await fs.access(cachePath).then(() => true).catch(() => false);
      
      if (exists) {
        logger.info(`Cache hit (disk): ${archiveId}`);
        this.memCache.set(cacheKey, cachePath);
        return cachePath;
      }
    }

    return null;
  }

  async setCachePath(archiveId, extractPath) {
    const cacheKey = `archive:${archiveId}`;
    
    // Add to memory cache
    this.memCache.set(cacheKey, extractPath);
    
    // Add to disk cache index
    this.cacheIndex.set(archiveId, extractPath);
    await this.saveIndex();
    
    logger.info(`Cached: ${archiveId} -> ${extractPath}`);
  }

  async saveIndex() {
    try {
      const indexPath = join(config.cacheDir, 'index.json');
      const index = Object.fromEntries(this.cacheIndex);
      await fs.writeFile(indexPath, JSON.stringify(index, null, 2));
    } catch (error) {
      logger.error('Failed to save cache index:', error);
    }
  }

  async cleanOldCache() {
    try {
      const now = Date.now();
      const maxAge = config.cacheTTL * 1000;
      
      for (const [archiveId, cachePath] of this.cacheIndex.entries()) {
        try {
          const stat = await fs.stat(cachePath);
          if (now - stat.mtimeMs > maxAge) {
            await fs.rm(cachePath, { recursive: true, force: true });
            this.cacheIndex.delete(archiveId);
            logger.info(`Cleaned old cache: ${archiveId}`);
          }
        } catch (error) {
          // Path doesn't exist, remove from index
          this.cacheIndex.delete(archiveId);
        }
      }
      
      await this.saveIndex();
    } catch (error) {
      logger.error('Failed to clean cache:', error);
    }
  }

  async getCacheSize() {
    let totalSize = 0;
    
    try {
      const files = await fs.readdir(config.cacheDir, { withFileTypes: true });
      
      for (const file of files) {
        if (file.isDirectory()) {
          const dirPath = join(config.cacheDir, file.name);
          totalSize += await this.getDirectorySize(dirPath);
        } else {
          const filePath = join(config.cacheDir, file.name);
          const stat = await fs.stat(filePath);
          totalSize += stat.size;
        }
      }
    } catch (error) {
      logger.error('Failed to calculate cache size:', error);
    }
    
    return totalSize;
  }

  async getDirectorySize(dirPath) {
    let size = 0;
    const files = await fs.readdir(dirPath, { withFileTypes: true });
    
    for (const file of files) {
      const fullPath = join(dirPath, file.name);
      const stat = await fs.stat(fullPath);
      
      if (file.isDirectory()) {
        size += await this.getDirectorySize(fullPath);
      } else {
        size += stat.size;
      }
    }
    
    return size;
  }

  clearMemoryCache() {
    this.memCache.flushAll();
    logger.info('Memory cache cleared');
  }

  async clearDiskCache() {
    try {
      const files = await fs.readdir(config.cacheDir);
      
      for (const file of files) {
        if (file !== 'index.json') {
          const filePath = join(config.cacheDir, file);
          await fs.rm(filePath, { recursive: true, force: true });
        }
      }
      
      this.cacheIndex.clear();
      await this.saveIndex();
      logger.info('Disk cache cleared');
    } catch (error) {
      logger.error('Failed to clear disk cache:', error);
    }
  }
}

export default new CacheService();