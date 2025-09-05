import { spawn } from 'child_process';
import tar from 'tar';
import fs from 'fs/promises';
import { createReadStream, createWriteStream } from 'fs';
import { pipeline } from 'stream/promises';
import { join } from 'path';
import crypto from 'crypto';
import logger from '../utils/logger.js';
import config from '../config.js';

class ExtractorService {
  constructor() {
    this.extractionProgress = new Map();
  }

  async extractArchive(archivePath, outputDir, onProgress) {
    const extractId = crypto.randomBytes(16).toString('hex');
    this.extractionProgress.set(extractId, { progress: 0, status: 'starting' });

    try {
      const archiveStat = await fs.stat(archivePath);
      const totalSize = archiveStat.size;
      let processedSize = 0;

      // Determine archive type
      const isZstd = archivePath.endsWith('.zst');
      const isGzip = archivePath.endsWith('.gz');

      logger.info(`Extracting archive: ${archivePath} (${totalSize} bytes)`);

      if (isZstd) {
        await this.extractZstdArchive(archivePath, outputDir, (progress) => {
          this.updateProgress(extractId, progress);
          if (onProgress) onProgress(progress);
        });
      } else if (isGzip) {
        await this.extractGzipArchive(archivePath, outputDir, (progress) => {
          this.updateProgress(extractId, progress);
          if (onProgress) onProgress(progress);
        });
      } else {
        throw new Error('Unsupported archive format');
      }

      this.updateProgress(extractId, 100, 'completed');
      logger.info(`Archive extracted successfully to: ${outputDir}`);
      
      return { extractId, outputDir };

    } catch (error) {
      this.updateProgress(extractId, -1, 'failed');
      logger.error(`Extraction failed: ${error.message}`);
      throw error;
    } finally {
      // Clean up progress tracking after 5 minutes
      setTimeout(() => {
        this.extractionProgress.delete(extractId);
      }, 300000);
    }
  }

  async extractZstdArchive(archivePath, outputDir, onProgress) {
    return new Promise((resolve, reject) => {
      // First decompress with zstd
      const zstdProcess = spawn('zstd', ['-d', '--stdout', archivePath]);
      
      let totalBytes = 0;
      let processedBytes = 0;

      // Monitor decompression progress
      zstdProcess.stderr.on('data', (data) => {
        const output = data.toString();
        const match = output.match(/(\d+\.?\d*)\s*%/);
        if (match) {
          const progress = parseFloat(match[1]);
          onProgress(progress * 0.5); // First 50% for decompression
        }
      });

      // Extract tar from decompressed stream
      const extractStream = tar.extract({
        cwd: outputDir,
        preserveOwner: false,
        onentry: (entry) => {
          processedBytes += entry.size;
          const progress = 50 + (processedBytes / totalBytes) * 50;
          onProgress(Math.min(progress, 100));
        }
      });

      zstdProcess.stdout.pipe(extractStream);

      extractStream.on('finish', () => {
        onProgress(100);
        resolve();
      });

      extractStream.on('error', reject);
      zstdProcess.on('error', reject);
    });
  }

  async extractGzipArchive(archivePath, outputDir, onProgress) {
    return new Promise((resolve, reject) => {
      const stream = createReadStream(archivePath);
      let processedBytes = 0;
      let totalBytes = 0;

      // Get total size for progress calculation
      fs.stat(archivePath).then(stat => {
        totalBytes = stat.size;
      });

      stream
        .pipe(tar.extract({
          cwd: outputDir,
          preserveOwner: false,
          onentry: (entry) => {
            processedBytes += entry.size;
            if (totalBytes > 0) {
              const progress = (processedBytes / totalBytes) * 100;
              onProgress(Math.min(progress, 100));
            }
          }
        }))
        .on('finish', () => {
          onProgress(100);
          resolve();
        })
        .on('error', reject);
    });
  }

  updateProgress(extractId, progress, status = 'extracting') {
    if (this.extractionProgress.has(extractId)) {
      this.extractionProgress.set(extractId, { progress, status });
    }
  }

  getProgress(extractId) {
    return this.extractionProgress.get(extractId) || { progress: -1, status: 'not_found' };
  }
}

export default new ExtractorService();