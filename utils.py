import re
import os
import json
import hashlib
import time
from urllib.parse import urljoin, urlparse, parse_qs
from urllib.robotparser import RobotFileParser
from pathlib import Path
import logging
from typing import Set, List, Dict, Optional
import asyncio
import aiohttp

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class URLFilter:
    """Filter and validate URLs"""
    
    # Patterns to exclude
    EXCLUDE_PATTERNS = [
        r'/login',
        r'/signin',
        r'/signup',
        r'/register',
        r'/download',
        r'/logout',
        r'\.pdf$',
        r'\.zip$',
        r'\.exe$',
        r'\.dmg$',
        r'\.msi$',
        r'\.tar\.gz$',
        r'\.rar$',
        r'mailto:',
        r'tel:',
        r'javascript:',
        r'#$'
    ]
    
    # File extensions to process
    ALLOWED_EXTENSIONS = {
        '.html', '.htm', '.php', '.asp', '.aspx', 
        '.jsp', '.css', '.js', '.json', '.xml'
    }
    
    IMAGE_EXTENSIONS = {
        '.jpg', '.jpeg', '.png', '.gif', '.webp', 
        '.svg', '.ico', '.bmp', '.avif'
    }
    
    @classmethod
    def should_scrape(cls, url: str, base_domain: str) -> bool:
        """Check if URL should be scraped"""
        try:
            parsed = urlparse(url)
            base_parsed = urlparse(base_domain)
            
            # Check if same domain
            if parsed.netloc != base_parsed.netloc:
                return False
            
            # Check exclude patterns
            for pattern in cls.EXCLUDE_PATTERNS:
                if re.search(pattern, url, re.IGNORECASE):
                    return False
            
            # Check query parameters for download/login indicators
            query_params = parse_qs(parsed.query)
            exclude_params = {'download', 'login', 'logout', 'signin', 'signup'}
            if any(param in query_params for param in exclude_params):
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error filtering URL {url}: {e}")
            return False
    
    @classmethod
    def get_url_hash(cls, url: str) -> str:
        """Generate hash for URL for storage"""
        return hashlib.md5(url.encode()).hexdigest()

class RobotsChecker:
    """Check robots.txt compliance"""
    
    def __init__(self):
        self.robots_cache = {}
        
    async def can_fetch(self, url: str, user_agent: str = '*') -> bool:
        """Check if URL can be fetched according to robots.txt"""
        try:
            parsed = urlparse(url)
            base_url = f"{parsed.scheme}://{parsed.netloc}"
            
            if base_url not in self.robots_cache:
                robots_url = f"{base_url}/robots.txt"
                rp = RobotFileParser()
                rp.set_url(robots_url)
                
                # Try to read robots.txt
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(robots_url, timeout=5) as response:
                            if response.status == 200:
                                content = await response.text()
                                rp.parse(content.splitlines())
                            else:
                                # No robots.txt, allow all
                                return True
                except:
                    # Error fetching robots.txt, allow by default
                    return True
                    
                self.robots_cache[base_url] = rp
            
            return self.robots_cache[base_url].can_fetch(user_agent, url)
            
        except Exception as e:
            logger.warning(f"Error checking robots.txt for {url}: {e}")
            return True  # Allow by default on error

class ScraperStats:
    """Track scraper statistics"""
    
    def __init__(self):
        self.start_time = time.time()
        self.pages_scraped = 0
        self.pages_failed = 0
        self.bytes_downloaded = 0
        self.domain_counts = {}
        
    def add_page(self, url: str, size: int):
        """Add a successfully scraped page"""
        self.pages_scraped += 1
        self.bytes_downloaded += size
        
        domain = urlparse(url).netloc
        self.domain_counts[domain] = self.domain_counts.get(domain, 0) + 1
        
    def add_failed(self):
        """Add a failed page"""
        self.pages_failed += 1
        
    def get_stats(self) -> dict:
        """Get current statistics"""
        elapsed = time.time() - self.start_time
        
        return {
            'pages_scraped': self.pages_scraped,
            'pages_failed': self.pages_failed,
            'bytes_downloaded': self.bytes_downloaded,
            'elapsed_seconds': elapsed,
            'pages_per_second': self.pages_scraped / elapsed if elapsed > 0 else 0,
            'domain_counts': self.domain_counts,
            'total_domains': len(self.domain_counts)
        }

def ensure_directories(*dirs):
    """Ensure directories exist"""
    for directory in dirs:
        Path(directory).mkdir(parents=True, exist_ok=True)

def save_json(data: dict, filepath: str):
    """Save data to JSON file"""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def load_json(filepath: str) -> dict:
    """Load data from JSON file"""
    if os.path.exists(filepath):
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}