import fs from 'fs/promises';
import { join } from 'path';
import crypto from 'crypto';
import config from '../config.js';
import extractorService from '../services/extractorService.js';
import cacheService from '../services/cacheService.js';
import logger from '../utils/logger.js';

class ArchiveController {
  async listArchives(req, res) {
    try {
      const files = await fs.readdir(config.archivesDir);
      const archives = [];

      for (const file of files) {
        if (file.endsWith('.tar.zst') || file.endsWith('.tar.gz')) {
          const filePath = join(config.archivesDir, file);
          const stat = await fs.stat(filePath);
          
          // Try to read metadata
          let metadata = {};
          const metadataPath = filePath.replace(/\.(tar\.zst|tar\.gz)$/, '_metadata.json');
          try {
            const metadataContent = await fs.readFile(metadataPath, 'utf-8');
            metadata = JSON.parse(metadataContent);
          } catch (e) {
            // No metadata file
          }

          archives.push({
            id: crypto.createHash('md5').update(file).digest('hex'),
            filename: file,
            size: stat.size,
            created: stat.birthtime,
            modified: stat.mtime,
            metadata: metadata
          });
        }
      }

      res.json({
        success: true,
        archives: archives.sort((a, b) => b.modified - a.modified),
        total: archives.length
      });
    } catch (error) {
      logger.error('Failed to list archives:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to list archives'
      });
    }
  }

  async extractArchive(req, res) {
    const { archiveId } = req.params;
    const io = req.app.get('io');

    try {
      // Find archive file
      const files = await fs.readdir(config.archivesDir);
      let archiveFile = null;

      for (const file of files) {
        const fileId = crypto.createHash('md5').update(file).digest('hex');
        if (fileId === archiveId) {
          archiveFile = file;
          break;
        }
      }

      if (!archiveFile) {
        return res.status(404).json({
          success: false,
          error: 'Archive not found'
        });
      }

      // Check cache
      const cachedPath = await cacheService.getCachePath(archiveId);
      if (cachedPath) {
        return res.json({
          success: true,
          path: cachedPath,
          cached: true
        });
      }

      // Extract archive
      const archivePath = join(config.archivesDir, archiveFile);
      const outputDir = join(config.cacheDir, archiveId);

      await fs.mkdir(outputDir, { recursive: true });

      const result = await extractorService.extractArchive(
        archivePath,
        outputDir,
        (progress) => {
          // Send progress updates via Socket.IO
          io.emit('extraction-progress', {
            archiveId,
            progress
          });
        }
      );

      // Cache the result
      await cacheService.setCachePath(archiveId, outputDir);

      res.json({
        success: true,
        path: outputDir,
        cached: false,
        extractId: result.extractId
      });

    } catch (error) {
      logger.error('Failed to extract archive:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to extract archive'
      });
    }
  }

  async getExtractionProgress(req, res) {
    const { extractId } = req.params;

    const progress = extractorService.getProgress(extractId);
    
    res.json({
      success: true,
      ...progress
    });
  }

  async clearCache(req, res) {
    try {
      await cacheService.clearDiskCache();
      cacheService.clearMemoryCache();

      res.json({
        success: true,
        message: 'Cache cleared successfully'
      });
    } catch (error) {
      logger.error('Failed to clear cache:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to clear cache'
      });
    }
  }

  async getCacheInfo(req, res) {
    try {
      const cacheSize = await cacheService.getCacheSize();
      
      res.json({
        success: true,
        size: cacheSize,
        maxSize: config.maxCacheSize,
        usage: (cacheSize / config.maxCacheSize) * 100
      });
    } catch (error) {
      logger.error('Failed to get cache info:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to get cache info'
      });
    }
  }
}

export default new ArchiveController();