import asyncio
import aiohttp
import aiofiles
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import os
import json
import time
from datetime import datetime
from typing import Set, Dict, Optional, Tuple
from tqdm.asyncio import tqdm
import logging
from utils import URLFilter, RobotsChecker, ScraperStats, save_json, ensure_directories
import hashlib

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
        self.asset_map: Dict[str, str] = {}  # Map original URLs to local paths
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
        ensure_directories(
            output_dir, 
            f"{output_dir}/html", 
            f"{output_dir}/assets",
            f"{output_dir}/css",
            f"{output_dir}/js",
            f"{output_dir}/images"
        )
        
    def get_asset_local_path(self, url: str, asset_type: str) -> str:
        """Generate local path for an asset"""
        url_hash = hashlib.md5(url.encode()).hexdigest()
        ext = os.path.splitext(urlparse(url).path)[1] or '.bin'
        
        # Determine subdirectory based on asset type
        if asset_type in ['image', 'img']:
            subdir = 'images'
        elif asset_type == 'css':
            subdir = 'css'
        elif asset_type in ['js', 'javascript']:
            subdir = 'js'
        else:
            subdir = 'assets'
        
        return f"{subdir}/{url_hash}{ext}"
    
    async def download_asset(self, session: aiohttp.ClientSession, url: str, asset_type: str) -> Optional[str]:
        """Download an asset and return its local path"""
        try:
            # Skip if already downloaded
            if url in self.asset_map:
                return self.asset_map[url]
            
            # Apply rate limiting
            domain = urlparse(url).netloc
            await self.apply_rate_limit(domain)
            
            async with self.semaphore:
                async with session.get(url, timeout=30, ssl=False) as response:
                    if response.status == 200:
                        content = await response.read()
                        
                        # Generate local path
                        local_path = self.get_asset_local_path(url, asset_type)
                        full_path = f"{self.output_dir}/{local_path}"
                        
                        # Save asset
                        os.makedirs(os.path.dirname(full_path), exist_ok=True)
                        
                        if asset_type in ['css', 'js']:
                            # Text assets
                            async with aiofiles.open(full_path, 'w', encoding='utf-8') as f:
                                await f.write(content.decode('utf-8', errors='ignore'))
                        else:
                            # Binary assets
                            async with aiofiles.open(full_path, 'wb') as f:
                                await f.write(content)
                        
                        # Store mapping
                        self.asset_map[url] = local_path
                        logger.debug(f"Downloaded asset: {url} -> {local_path}")
                        
                        return local_path
                    else:
                        logger.warning(f"Failed to download asset {url}: HTTP {response.status}")
                        return None
                        
        except Exception as e:
            logger.error(f"Error downloading asset {url}: {e}")
            return None
    
    async def rewrite_html_urls(self, html: str, base_url: str, session: aiohttp.ClientSession) -> str:
        """Rewrite URLs in HTML to point to local assets"""
        soup = BeautifulSoup(html, 'lxml')
        
        # Process different types of assets
        asset_tasks = []
        
        # Images
        for img in soup.find_all(['img', 'source']):
            for attr in ['src', 'srcset', 'data-src', 'data-srcset']:
                if img.get(attr):
                    urls = []
                    # Handle srcset which can have multiple URLs
                    if 'srcset' in attr:
                        srcset_parts = img[attr].split(',')
                        for part in srcset_parts:
                            url_part = part.strip().split(' ')[0]
                            if url_part:
                                urls.append(url_part)
                    else:
                        urls.append(img[attr])
                    
                    for url in urls:
                        if url and not url.startswith('data:'):
                            absolute_url = urljoin(base_url, url)
                            asset_tasks.append((img, attr, url, absolute_url, 'image'))
        
        # CSS files
        for link in soup.find_all('link', rel='stylesheet'):
            if link.get('href'):
                url = link['href']
                absolute_url = urljoin(base_url, url)
                asset_tasks.append((link, 'href', url, absolute_url, 'css'))
        
        # JavaScript files
        for script in soup.find_all('script'):
            if script.get('src'):
                url = script['src']
                absolute_url = urljoin(base_url, url)
                asset_tasks.append((script, 'src', url, absolute_url, 'js'))
        
        # CSS in style tags (for url() references)
        for style in soup.find_all('style'):
            if style.string:
                style.string = await self.rewrite_css_urls(style.string, base_url, session)
        
        # Inline styles with url()
        for element in soup.find_all(style=True):
            element['style'] = await self.rewrite_css_urls(element['style'], base_url, session)
        
        # Download assets and update URLs
        if not self.skip_assets:
            for element, attr, original_url, absolute_url, asset_type in asset_tasks:
                local_path = await self.download_asset(session, absolute_url, asset_type)
                if local_path:
                    # Update the URL to point to local file
                    # Use relative path from html directory
                    relative_path = f"../{local_path}"
                    
                    if 'srcset' in attr:
                        # Handle srcset specially
                        srcset_parts = element[attr].split(',')
                        new_srcset = []
                        for part in srcset_parts:
                            part_items = part.strip().split(' ')
                            if part_items[0] == original_url:
                                part_items[0] = relative_path
                            new_srcset.append(' '.join(part_items))
                        element[attr] = ', '.join(new_srcset)
                    else:
                        element[attr] = relative_path
        
        # Process links to make them work locally
        for a in soup.find_all('a', href=True):
            href = a['href']
            if not href.startswith(('#', 'javascript:', 'mailto:', 'tel:')):
                absolute_url = urljoin(base_url, href)
                # Check if we have this page
                if absolute_url in self.visited_urls:
                    # Link to local HTML file
                    url_hash = URLFilter.get_url_hash(absolute_url)
                    a['href'] = f"{url_hash}.html"
                else:
                    # Keep as external link but make it absolute
                    a['href'] = absolute_url
        
        return str(soup)
    
    async def rewrite_css_urls(self, css_content: str, base_url: str, session: aiohttp.ClientSession) -> str:
        """Rewrite URLs in CSS content"""
        import re
        
        # Find all url() references
        url_pattern = r'urlKATEX_INLINE_OPEN[\'"]?([^\'")]+)[\'"]?KATEX_INLINE_CLOSE'
        
        async def replace_url(match):
            url = match.group(1)
            if not url.startswith('data:'):
                absolute_url = urljoin(base_url, url)
                local_path = await self.download_asset(session, absolute_url, 'image')
                if local_path:
                    # Use relative path from css directory
                    relative_path = f"../{local_path}"
                    return f'url("{relative_path}")'
            return match.group(0)
        
        # Process URLs sequentially (regex doesn't support async)
        urls = re.findall(url_pattern, css_content)
        for url in urls:
            if not url.startswith('data:'):
                absolute_url = urljoin(base_url, url)
                local_path = await self.download_asset(session, absolute_url, 'image')
                if local_path:
                    relative_path = f"../{local_path}"
                    css_content = css_content.replace(f'url({url})', f'url({relative_path})')
                    css_content = css_content.replace(f'url("{url}")', f'url("{relative_path}")')
                    css_content = css_content.replace(f"url('{url}')", f'url("{relative_path}")')
        
        return css_content
    
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
    
    async def fetch_page(self, session: aiohttp.ClientSession, url: str) -> Optional[Tuple[str, str]]:
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
            
            # Extract from links
            for tag in soup.find_all(['a', 'area']):
                url = tag.get('href')
                if url:
                    absolute_url = urljoin(base_url, url)
                    if URLFilter.should_scrape(absolute_url, self.base_domain):
                        urls.add(absolute_url)
                        
        except Exception as e:
            logger.error(f"Error extracting URLs: {e}")
            
        return urls
    
    async def save_page_content(self, url: str, content: str, content_type: str) -> Optional[str]:
        """Save page content to disk"""
        try:
            url_hash = URLFilter.get_url_hash(url)
            
            # Always save HTML files in the html directory
            if 'html' in content_type or 'text' in content_type:
                filepath = f"{self.output_dir}/html/{url_hash}.html"
            else:
                # Other content types
                ext = '.txt'
                if 'json' in content_type:
                    ext = '.json'
                elif 'xml' in content_type:
                    ext = '.xml'
                filepath = f"{self.output_dir}/html/{url_hash}{ext}"
            
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
        
        # Process HTML content
        if 'html' in content_type:
            # Rewrite URLs to point to local assets
            content = await self.rewrite_html_urls(content, url, session)
        
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
            'pages': self.scraped_data,
            'asset_map': self.asset_map  # Save asset mappings
        }, metadata_path)
        
        # Log summary
        logger.info("=" * 60)
        logger.info("Scraping Summary:")
        logger.info(f"  Pages scraped: {self.pages_scraped_count}/{self.max_pages}")
        logger.info(f"  Assets downloaded: {len(self.asset_map)}")
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