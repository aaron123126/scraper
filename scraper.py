import asyncio
import aiohttp
import aiofiles
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import os
import json
import time
from datetime import datetime
from typing import Set, Dict, Optional
from tqdm.asyncio import tqdm
import logging
from utils import URLFilter, RobotsChecker, ScraperStats, save_json, ensure_directories

logger = logging.getLogger(__name__)

class WebScraper:
    def __init__(self, 
                 start_url: str, 
                 output_dir: str, 
                 max_workers: int = 10, 
                 max_depth: int = 3,
                 max_pages: int = 100,
                 pages_per_domain: int = 50,
                 respect_robots: bool = True,
                 request_delay: float = 0.5,
                 skip_assets: bool = False):
        
        self.start_url = start_url
        self.base_domain = f"{urlparse(start_url).scheme}://{urlparse(start_url).netloc}"
        self.output_dir = output_dir
        self.max_workers = max_workers
        self.max_depth = max_depth
        self.max_pages = max_pages
        self.pages_per_domain = pages_per_domain
        self.respect_robots = respect_robots
        self.request_delay = request_delay
        self.skip_assets = skip_assets
        
        self.visited_urls: Set[str] = set()
        self.scraped_data: Dict[str, dict] = {}
        self.queue = asyncio.Queue()
        self.semaphore = asyncio.Semaphore(max_workers)
        self.robots_checker = RobotsChecker() if respect_robots else None
        self.stats = ScraperStats()
        self.domain_counts: Dict[str, int] = {}
        self.last_request_time: Dict[str, float] = {}
        
        # Page limit tracking
        self.pages_scraped_count = 0
        self.should_stop = False
        
        # Ensure output directories exist
        ensure_directories(output_dir, f"{output_dir}/html", f"{output_dir}/assets")
        
    async def check_limits(self, url: str) -> bool:
        """Check if we should continue scraping based on limits"""
        # Check global page limit
        if self.pages_scraped_count >= self.max_pages:
            logger.info(f"Reached maximum page limit: {self.max_pages}")
            self.should_stop = True
            return False
            
        # Check per-domain limit
        domain = urlparse(url).netloc
        domain_count = self.domain_counts.get(domain, 0)
        
        if domain_count >= self.pages_per_domain:
            logger.warning(f"Reached limit for domain {domain}: {self.pages_per_domain} pages")
            return False
            
        return True
    
    async def apply_rate_limit(self, domain: str):
        """Apply rate limiting per domain"""
        if self.request_delay > 0:
            last_time = self.last_request_time.get(domain, 0)
            elapsed = time.time() - last_time
            
            if elapsed < self.request_delay:
                await asyncio.sleep(self.request_delay - elapsed)
            
            self.last_request_time[domain] = time.time()
    
    async def fetch_page(self, session: aiohttp.ClientSession, url: str) -> Optional[tuple]:
        """Fetch a single page"""
        try:
            # Check robots.txt
            if self.robots_checker:
                can_fetch = await self.robots_checker.can_fetch(url, 'WebArchiver/1.0')
                if not can_fetch:
                    logger.info(f"Robots.txt disallows: {url}")
                    return None
            
            # Apply rate limiting
            domain = urlparse(url).netloc
            await self.apply_rate_limit(domain)
            
            async with self.semaphore:
                async with session.get(url, timeout=30, ssl=False) as response:
                    if response.status == 200:
                        content = await response.text()
                        content_type = response.headers.get('Content-Type', '')
                        return content, content_type
                    else:
                        logger.warning(f"HTTP {response.status} for {url}")
                        return None
                        
        except asyncio.TimeoutError:
            logger.warning(f"Timeout fetching {url}")
            self.stats.add_failed()
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            self.stats.add_failed()
        return None
    
    def extract_urls(self, html: str, base_url: str) -> Set[str]:
        """Extract all URLs from HTML"""
        urls = set()
        try:
            soup = BeautifulSoup(html, 'lxml')
            
            # Extract from various tags
            for tag in soup.find_all(['a', 'link', 'script', 'img']):
                url = None
                if tag.name == 'a':
                    url = tag.get('href')
                elif tag.name == 'link':
                    url = tag.get('href')
                elif tag.name == 'script' and not self.skip_assets:
                    url = tag.get('src')
                elif tag.name == 'img' and not self.skip_assets:
                    url = tag.get('src')
                
                if url:
                    absolute_url = urljoin(base_url, url)
                    if URLFilter.should_scrape(absolute_url, self.base_domain):
                        urls.add(absolute_url)
                        
        except Exception as e:
            logger.error(f"Error extracting URLs: {e}")
            
        return urls
    
    async def save_page_content(self, url: str, content: str, content_type: str):
        """Save page content to disk"""
        try:
            url_hash = URLFilter.get_url_hash(url)
            
            # Determine file extension
            if 'html' in content_type:
                ext = '.html'
                subdir = 'html'
            elif 'css' in content_type and not self.skip_assets:
                ext = '.css'
                subdir = 'assets'
            elif 'javascript' in content_type and not self.skip_assets:
                ext = '.js'
                subdir = 'assets'
            elif 'json' in content_type:
                ext = '.json'
                subdir = 'assets'
            else:
                ext = '.txt'
                subdir = 'html'
            
            filepath = f"{self.output_dir}/{subdir}/{url_hash}{ext}"
            
            async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                await f.write(content)
            
            return filepath
            
        except Exception as e:
            logger.error(f"Error saving content for {url}: {e}")
            return None
    
    async def process_url(self, session: aiohttp.ClientSession, url: str, depth: int):
        """Process a single URL"""
        if self.should_stop:
            return
            
        if url in self.visited_urls or depth > self.max_depth:
            return
        
        # Check limits before processing
        if not await self.check_limits(url):
            return
        
        self.visited_urls.add(url)
        
        # Fetch page
        result = await self.fetch_page(session, url)
        if not result:
            return
        
        content, content_type = result
        
        # Update counters
        self.pages_scraped_count += 1
        domain = urlparse(url).netloc
        self.domain_counts[domain] = self.domain_counts.get(domain, 0) + 1
        
        # Update stats
        self.stats.add_page(url, len(content))
        
        # Save content
        filepath = await self.save_page_content(url, content, content_type)
        
        # Store metadata
        self.scraped_data[url] = {
            'url': url,
            'timestamp': datetime.now().isoformat(),
            'content_type': content_type,
            'filepath': filepath,
            'depth': depth,
            'size': len(content),
            'domain': domain
        }
        
        # Log progress
        if self.pages_scraped_count % 10 == 0:
            logger.info(f"Progress: {self.pages_scraped_count}/{self.max_pages} pages scraped")
        
        # Extract and queue new URLs if HTML
        if 'html' in content_type and not self.should_stop:
            new_urls = self.extract_urls(content, url)
            for new_url in new_urls:
                if new_url not in self.visited_urls and not self.should_stop:
                    await self.queue.put((new_url, depth + 1))
    
    async def worker(self, session: aiohttp.ClientSession, pbar: tqdm):
        """Worker to process URLs from queue"""
        while not self.should_stop:
            try:
                url, depth = await asyncio.wait_for(self.queue.get(), timeout=5)
                
                if self.should_stop:
                    break
                    
                await self.process_url(session, url, depth)
                pbar.update(1)
                pbar.set_description(f"Pages: {self.pages_scraped_count}/{self.max_pages}")
                self.queue.task_done()
                
            except asyncio.TimeoutError:
                if self.queue.empty():
                    break
            except Exception as e:
                logger.error(f"Worker error: {e}")
    
    async def run(self):
        """Run the scraper"""
        logger.info(f"Starting scraper for {self.start_url}")
        logger.info(f"Limits: max_pages={self.max_pages}, pages_per_domain={self.pages_per_domain}")
        logger.info(f"Settings: max_depth={self.max_depth}, workers={self.max_workers}")
        
        # Initialize session
        timeout = aiohttp.ClientTimeout(total=60)
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
        
        async with aiohttp.ClientSession(
            timeout=timeout, 
            connector=connector,
            headers={'User-Agent': 'Mozilla/5.0 (compatible; WebArchiver/1.0)'}
        ) as session:
            
            # Add start URL to queue
            await self.queue.put((self.start_url, 0))
            
            # Create progress bar
            with tqdm(
                desc=f"Scraping (0/{self.max_pages})", 
                unit="pages",
                total=self.max_pages
            ) as pbar:
                # Start workers
                workers = [
                    asyncio.create_task(self.worker(session, pbar))
                    for _ in range(self.max_workers)
                ]
                
                # Wait for queue to be processed or limit reached
                while not self.queue.empty() and not self.should_stop:
                    await asyncio.sleep(0.5)
                
                # Wait for remaining tasks
                await self.queue.join()
                
                # Signal stop and cancel workers
                self.should_stop = True
                for worker in workers:
                    worker.cancel()
                
                await asyncio.gather(*workers, return_exceptions=True)
        
        # Get final stats
        final_stats = self.stats.get_stats()
        
        # Save metadata
        metadata_path = f"{self.output_dir}/metadata.json"
        save_json({
            'start_url': self.start_url,
            'total_pages': len(self.scraped_data),
            'pages_scraped': self.pages_scraped_count,
            'max_pages_limit': self.max_pages,
            'pages_per_domain_limit': self.pages_per_domain,
            'timestamp': datetime.now().isoformat(),
            'stats': final_stats,
            'domain_counts': self.domain_counts,
            'pages': self.scraped_data
        }, metadata_path)
        
        # Log summary
        logger.info("=" * 60)
        logger.info("Scraping Summary:")
        logger.info(f"  Pages scraped: {self.pages_scraped_count}/{self.max_pages}")
        logger.info(f"  Pages failed: {final_stats['pages_failed']}")
        logger.info(f"  Data downloaded: {final_stats['bytes_downloaded']:,} bytes")
        logger.info(f"  Time elapsed: {final_stats['elapsed_seconds']:.2f} seconds")
        logger.info(f"  Pages/second: {final_stats['pages_per_second']:.2f}")
        logger.info(f"  Domains scraped: {final_stats['total_domains']}")
        
        if self.domain_counts:
            logger.info("\nTop domains:")
            for domain, count in sorted(self.domain_counts.items(), 
                                       key=lambda x: x[1], reverse=True)[:5]:
                logger.info(f"    {domain}: {count} pages")
        
        logger.info("=" * 60)
        
        return self.scraped_data