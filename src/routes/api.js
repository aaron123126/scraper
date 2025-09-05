import { Router } from 'express';
import archiveController from '../controllers/archiveController.js';
import multer from 'multer';
import config from '../config.js';

const router = Router();

// Configure multer for file uploads
const storage = multer.diskStorage({
  destination: config.archivesDir,
  filename: (req, file, cb) => {
    const timestamp = Date.now();
    cb(null, `upload_${timestamp}_${file.originalname}`);
  }
});

const upload = multer({
  storage,
  limits: { fileSize: config.maxUploadSize },
  fileFilter: (req, file, cb) => {
    if (file.originalname.match(/\.(tar\.zst|tar\.gz)$/)) {
      cb(null, true);
    } else {
      cb(new Error('Only .tar.zst and .tar.gz files are allowed'));
    }
  }
});

// Archive management
router.get('/archives', archiveController.listArchives);
router.post('/archives/:archiveId/extract', archiveController.extractArchive);
router.get('/extraction/:extractId/progress', archiveController.getExtractionProgress);

// Cache management
router.delete('/cache', archiveController.clearCache);
router.get('/cache/info', archiveController.getCacheInfo);

// Upload archive
router.post('/upload', upload.single('archive'), (req, res) => {
  if (!req.file) {
    return res.status(400).json({
      success: false,
      error: 'No file uploaded'
    });
  }

  res.json({
    success: true,
    file: {
      filename: req.file.filename,
      size: req.file.size,
      originalName: req.file.originalname
    }
  });
});

export default router;