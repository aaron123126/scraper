import os
import re
from pathlib import Path
from typing import Optional, Tuple
import logging
from PIL import Image
import htmlmin
from csscompressor import compress as css_compress
from jsmin import jsmin
import subprocess
import asyncio
import aiofiles

logger = logging.getLogger(__name__)

class ContentOptimizer:
    def __init__(self, image_quality: int = 85, max_image_width: int = 1920):
        self.image_quality = image_quality
        self.max_image_width = max_image_width
    
    async def optimize_html(self, filepath: str) -> int:
        """Minify HTML file"""
        try:
            async with aiofiles.open(filepath, 'r', encoding='utf-8') as f:
                content = await f.read()
            
            # Remove comments
            content = re.sub(r'<!--.*?-->', '', content, flags=re.DOTALL)
            
            # Minify
            minified = htmlmin.minify(content, 
                remove_comments=True,
                remove_empty_space=True,
                remove_all_empty_space=False,
                reduce_boolean_attributes=True,
                remove_optional_attribute_quotes=False)
            
            async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                await f.write(minified)
            
            return len(content) - len(minified)
            
        except Exception as e:
            logger.error(f"Error optimizing HTML {filepath}: {e}")
            return 0
    
    async def optimize_css(self, filepath: str) -> int:
        """Minify CSS file"""
        try:
            async with aiofiles.open(filepath, 'r', encoding='utf-8') as f:
                content = await f.read()
            
            minified = css_compress(content)
            
            async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                await f.write(minified)
            
            return len(content) - len(minified)
            
        except Exception as e:
            logger.error(f"Error optimizing CSS {filepath}: {e}")
            return 0
    
    async def optimize_js(self, filepath: str) -> int:
        """Minify JavaScript file"""
        try:
            async with aiofiles.open(filepath, 'r', encoding='utf-8') as f:
                content = await f.read()
            
            minified = jsmin(content)
            
            async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                await f.write(minified)
            
            return len(content) - len(minified)
            
        except Exception as e:
            logger.error(f"Error optimizing JS {filepath}: {e}")
            return 0
    
    def optimize_image(self, filepath: str) -> int:
        """Optimize image file"""
        try:
            original_size = os.path.getsize(filepath)
            
            with Image.open(filepath) as img:
                # Convert RGBA to RGB if necessary
                if img.mode in ('RGBA', 'LA', 'P'):
                    rgb_img = Image.new('RGB', img.size, (255, 255, 255))
                    rgb_img.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
                    img = rgb_img
                
                # Resize if too large
                if img.width > self.max_image_width:
                    ratio = self.max_image_width / img.width
                    new_height = int(img.height * ratio)
                    img = img.resize((self.max_image_width, new_height), Image.Resampling.LANCZOS)
                
                # Save optimized image
                output_path = filepath.replace('.png', '.jpg').replace('.bmp', '.jpg')
                
                # Try to save as WebP if available
                try:
                    webp_path = Path(filepath).with_suffix('.webp')
                    img.save(webp_path, 'WEBP', quality=self.image_quality, method=6)
                    
                    # Remove original if WebP is smaller
                    if os.path.getsize(webp_path) < original_size * 0.9:
                        os.remove(filepath)
                        return original_size - os.path.getsize(webp_path)
                    else:
                        os.remove(webp_path)
                except:
                    pass
                
                # Fallback to JPEG
                img.save(output_path, 'JPEG', quality=self.image_quality, optimize=True)
                
                if output_path != filepath:
                    os.remove(filepath)
                
                return original_size - os.path.getsize(output_path)
                
        except Exception as e:
            logger.error(f"Error optimizing image {filepath}: {e}")
            return 0
    
    async def optimize_svg(self, filepath: str) -> int:
        """Optimize SVG file using SVGO if available"""
        try:
            original_size = os.path.getsize(filepath)
            
            # Try using SVGO if installed
            result = await asyncio.create_subprocess_exec(
                'svgo', filepath, '-o', filepath,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await result.communicate()
            
            return original_size - os.path.getsize(filepath)
            
        except FileNotFoundError:
            # SVGO not installed, do basic optimization
            try:
                async with aiofiles.open(filepath, 'r', encoding='utf-8') as f:
                    content = await f.read()
                
                # Remove comments and unnecessary whitespace
                content = re.sub(r'<!--.*?-->', '', content, flags=re.DOTALL)
                content = re.sub(r'>\s+<', '><', content)
                content = re.sub(r'\s+', ' ', content)
                
                async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                    await f.write(content)
                
                return original_size - os.path.getsize(filepath)
                
            except Exception as e:
                logger.error(f"Error optimizing SVG {filepath}: {e}")
                return 0