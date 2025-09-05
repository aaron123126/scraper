import os
import asyncio
import subprocess
import shutil
from pathlib import Path
from typing import Dict, List
from datetime import datetime
import logging
from tqdm.asyncio import tqdm
from optimizer import ContentOptimizer
from utils import save_json, load_json

logger = logging.getLogger(__name__)

class WebCompressor:
    def __init__(self, source_dir: str, archive_dir: str, compression_level: int = 19):
        self.source_dir = source_dir
        self.archive_dir = archive_dir
        self.compression_level = compression_level
        self.optimizer = ContentOptimizer()
        
        Path(archive_dir).mkdir(parents=True, exist_ok=True)
    
    async def optimize_directory(self, directory: str) -> Dict[str, int]:
        """Optimize all files in directory"""
        stats = {
            'html_saved': 0,
            'css_saved': 0,
            'js_saved': 0,
            'image_saved': 0,
            'svg_saved': 0,
            'total_saved': 0
        }
        
        # Get all files
        files_to_optimize = []
        for root, _, files in os.walk(directory):
            for file in files:
                filepath = os.path.join(root, file)
                files_to_optimize.append(filepath)
        
        # Process files with progress bar
        with tqdm(total=len(files_to_optimize), desc="Optimizing files") as pbar:
            tasks = []
            
            for filepath in files_to_optimize:
                ext = Path(filepath).suffix.lower()
                
                if ext in ['.html', '.htm']:
                    task = self.optimize_file(filepath, 'html', stats, pbar)
                elif ext == '.css':
                    task = self.optimize_file(filepath, 'css', stats, pbar)
                elif ext in ['.js', '.json']:
                    task = self.optimize_file(filepath, 'js', stats, pbar)
                elif ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp']:
                    task = self.optimize_file(filepath, 'image', stats, pbar)
                elif ext == '.svg':
                    task = self.optimize_file(filepath, 'svg', stats, pbar)
                else:
                    pbar.update(1)
                    continue
                
                tasks.append(task)
            
            # Run optimization tasks concurrently
            await asyncio.gather(*tasks)
        
        stats['total_saved'] = sum(v for k, v in stats.items() if k != 'total_saved')
        return stats
    
    async def optimize_file(self, filepath: str, file_type: str, stats: Dict, pbar: tqdm):
        """Optimize a single file"""
        saved = 0
        
        try:
            if file_type == 'html':
                saved = await self.optimizer.optimize_html(filepath)
                stats['html_saved'] += saved
            elif file_type == 'css':
                saved = await self.optimizer.optimize_css(filepath)
                stats['css_saved'] += saved
            elif file_type == 'js':
                saved = await self.optimizer.optimize_js(filepath)
                stats['js_saved'] += saved
            elif file_type == 'image':
                saved = await asyncio.get_event_loop().run_in_executor(
                    None, self.optimizer.optimize_image, filepath
                )
                stats['image_saved'] += saved
            elif file_type == 'svg':
                saved = await self.optimizer.optimize_svg(filepath)
                stats['svg_saved'] += saved
        except Exception as e:
            logger.error(f"Error optimizing {filepath}: {e}")
        
        pbar.update(1)
    
    async def create_archive(self, optimized_dir: str) -> str:
        """Create compressed archive using Zstandard"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        archive_name = f"web_archive_{timestamp}.tar.zst"
        archive_path = os.path.join(self.archive_dir, archive_name)
        
        logger.info(f"Creating archive: {archive_path}")
        
        try:
            # Check if zstd is available
            zstd_check = await asyncio.create_subprocess_exec(
                'which', 'zstd',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await zstd_check.communicate()
            
            if zstd_check.returncode == 0:
                # Use zstd with tar in a single command
                logger.info("Using zstd compression...")
                
                # Create tar archive with zstd compression directly
                compress_process = await asyncio.create_subprocess_exec(
                    'tar',
                    '--use-compress-program=zstd -' + str(self.compression_level),
                    '-cf', archive_path,
                    '-C', optimized_dir,
                    '.',
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                stdout, stderr = await compress_process.communicate()
                
                if compress_process.returncode != 0:
                    error_msg = stderr.decode() if stderr else "Unknown error"
                    raise Exception(f"Compression failed: {error_msg}")
                
                # Verify archive was created
                if not os.path.exists(archive_path):
                    raise Exception("Archive file was not created")
                
                # Get archive size
                archive_size = os.path.getsize(archive_path)
                logger.info(f"Archive created successfully: {archive_path} ({archive_size:,} bytes)")
                
            else:
                # Fallback to gzip if zstd is not available
                logger.warning("zstd not found, falling back to gzip compression...")
                archive_name = f"web_archive_{timestamp}.tar.gz"
                archive_path = os.path.join(self.archive_dir, archive_name)
                
                compress_process = await asyncio.create_subprocess_exec(
                    'tar', '-czf', archive_path,
                    '-C', optimized_dir, '.',
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                stdout, stderr = await compress_process.communicate()
                
                if compress_process.returncode != 0:
                    error_msg = stderr.decode() if stderr else "Unknown error"
                    raise Exception(f"Compression failed: {error_msg}")
            
            return archive_path
            
        except Exception as e:
            logger.error(f"Error creating archive: {e}")
            
            # Try Python's built-in compression as last resort
            logger.info("Falling back to Python's built-in compression...")
            try:
                import tarfile
                import gzip
                
                archive_name = f"web_archive_{timestamp}.tar.gz"
                archive_path = os.path.join(self.archive_dir, archive_name)
                
                with tarfile.open(archive_path, "w:gz", compresslevel=9) as tar:
                    tar.add(optimized_dir, arcname=".")
                
                logger.info(f"Archive created with Python tarfile: {archive_path}")
                return archive_path
                
            except Exception as fallback_error:
                logger.error(f"All compression methods failed: {fallback_error}")
                raise
    
    async def compress(self) -> Dict:
        """Main compression workflow"""
        logger.info("Starting compression workflow")
        
        # Step 1: Optimize all files
        logger.info("Step 1: Optimizing files...")
        optimization_stats = await self.optimize_directory(self.source_dir)
        
        logger.info(f"Optimization complete. Total saved: {optimization_stats['total_saved']:,} bytes")
        
        # Step 2: Create compressed archive
        logger.info("Step 2: Creating compressed archive...")
        archive_path = await self.create_archive(self.source_dir)
        
        # Calculate compression ratio
        original_size = sum(
            os.path.getsize(os.path.join(root, file))
            for root, _, files in os.walk(self.source_dir)
            for file in files
        )
        compressed_size = os.path.getsize(archive_path)
        compression_ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0
        
        # Save compression report
        report = {
            'timestamp': datetime.now().isoformat(),
            'source_directory': self.source_dir,
            'archive_path': archive_path,
            'optimization_stats': optimization_stats,
            'original_size': original_size,
            'compressed_size': compressed_size,
            'compression_ratio': f"{compression_ratio:.2f}%"
        }
        
        report_path = os.path.join(self.archive_dir, 'compression_report.json')
        save_json(report, report_path)
        
        logger.info(f"Compression complete. Ratio: {compression_ratio:.2f}%")
        
        return report