#!/usr/bin/env python3
# server.py

import os
import json
import asyncio
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import aiohttp
from aiohttp import web
import aiofiles
import subprocess
from dotenv import load_dotenv, set_key

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WebArchiveServer:
    def __init__(self, port=8080, scraped_data_dir='./scraped_data', archives_dir='./archives'):
        self.port = port
        self.scraped_data_dir = Path(scraped_data_dir)
        self.archives_dir = Path(archives_dir)
        self.app = web.Application()
        self.active_scrape = None
        self.setup_routes()
        
    def setup_routes(self):
        """Setup server routes"""
        # API routes
        self.app.router.add_get('/api/runs', self.get_runs)
        self.app.router.add_get('/api/run/{run_id}', self.get_run_details)
        self.app.router.add_get('/api/run/{run_id}/pages', self.get_run_pages)
        self.app.router.add_get('/api/run/{run_id}/page/{page_hash}', self.get_page_content)
        self.app.router.add_get('/api/run/{run_id}/preview/{page_hash}', self.preview_page)
        self.app.router.add_get('/api/run/{run_id}/stats', self.get_run_stats)
        self.app.router.add_get('/api/archives', self.get_archives)
        
        # Global search
        self.app.router.add_get('/api/search', self.global_search)
        
        # Scraping controls
        self.app.router.add_post('/api/scrape/start', self.start_scrape)
        self.app.router.add_get('/api/scrape/status', self.get_scrape_status)
        self.app.router.add_post('/api/scrape/stop', self.stop_scrape)
        self.app.router.add_get('/api/scrape/config', self.get_scrape_config)
        
        # Static files
        self.app.router.add_static('/', path='public', name='static', show_index=True)
        
    async def global_search(self, request):
        """Search across all runs"""
        query = request.query.get('q', '').lower()
        limit = int(request.query.get('limit', 100))
        
        if not query:
            return web.json_response({'results': []})
        
        results = []
        
        if self.scraped_data_dir.exists():
            for run_dir in self.scraped_data_dir.iterdir():
                if run_dir.is_dir() and run_dir.name.replace('_', '').isdigit():
                    metadata_file = run_dir / 'metadata.json'
                    if metadata_file.exists():
                        with open(metadata_file, 'r') as f:
                            metadata = json.load(f)
                        
                        # Search in pages
                        for url, page_data in metadata.get('pages', {}).items():
                            if query in url.lower() or query in page_data.get('domain', '').lower():
                                results.append({
                                    'run_id': run_dir.name,
                                    'url': url,
                                    'hash': self.get_url_hash(url),
                                    'domain': page_data.get('domain', ''),
                                    'timestamp': page_data.get('timestamp', ''),
                                    'content_type': page_data.get('content_type', ''),
                                    'size': page_data.get('size', 0)
                                })
                                
                                if len(results) >= limit:
                                    break
                
                if len(results) >= limit:
                    break
        
        return web.json_response({
            'results': results[:limit],
            'total': len(results),
            'query': query
        })
    
    async def preview_page(self, request):
        """Preview page as rendered HTML"""
        run_id = request.match_info['run_id']
        page_hash = request.match_info['page_hash']
        
        run_dir = self.scraped_data_dir / run_id
        
        # Try to find the HTML file
        for ext in ['.html', '.htm', '.txt']:
            html_file = run_dir / 'html' / f"{page_hash}{ext}"
            if html_file.exists():
                async with aiofiles.open(html_file, 'r', encoding='utf-8') as f:
                    content = await f.read()
                
                # Set base tag for relative URLs to work
                if '<head>' in content:
                    # Insert base tag to help with asset loading
                    base_path = f"/static/{run_id}/"
                    base_tag = f'<base href="{base_path}">'
                    content = content.replace('<head>', f'<head>{base_tag}')
                
                # Return as HTML response for direct rendering
                return web.Response(text=content, content_type='text/html')
        
        # If not found, return a proper 404 page
        error_html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Page Not Found</title>
            <style>
                body {
                    font-family: -apple-system, BlinkMacSystemFont, sans-serif;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    height: 100vh;
                    margin: 0;
                    background: #f3f4f6;
                }
                .error-container {
                    text-align: center;
                    padding: 2rem;
                    background: white;
                    border-radius: 0.5rem;
                    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                }
                h1 { color: #ef4444; }
                p { color: #6b7280; }
            </style>
        </head>
        <body>
            <div class="error-container">
                <h1>404 - Page Not Found</h1>
                <p>The requested page could not be found in the archive.</p>
                <p>It may not have been scraped or could have been filtered out.</p>
            </div>
        </body>
        </html>
        """
        return web.Response(text=error_html, content_type='text/html', status=404)
    
    async def start_scrape(self, request):
        """Start a new scraping job"""
        if self.active_scrape:
            return web.json_response({
                'error': 'A scrape is already in progress'
            }, status=400)
        
        try:
            data = await request.json()
            
            # Update .env file with new configuration
            env_file = '.env'
            for key, value in data.items():
                if key in ['START_URL', 'MAX_WORKERS', 'MAX_DEPTH', 'MAX_PAGES', 
                          'PAGES_PER_DOMAIN', 'IMAGE_QUALITY', 'MAX_IMAGE_WIDTH',
                          'COMPRESSION_LEVEL', 'SKIP_ASSETS', 'RESPECT_ROBOTS_TXT', 
                          'REQUEST_DELAY']:
                    set_key(env_file, key, str(value))
            
            # Start the scraping process
            self.active_scrape = await asyncio.create_subprocess_exec(
                'python', 'main.py',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            # Start monitoring task
            asyncio.create_task(self.monitor_scrape())
            
            return web.json_response({
                'status': 'started',
                'pid': self.active_scrape.pid
            })
            
        except Exception as e:
            logger.error(f"Error starting scrape: {e}")
            return web.json_response({
                'error': str(e)
            }, status=500)
    
    async def monitor_scrape(self):
        """Monitor active scrape process"""
        if self.active_scrape:
            await self.active_scrape.wait()
            self.active_scrape = None
            logger.info("Scraping process completed")
    
    async def get_scrape_status(self, request):
        """Get current scrape status"""
        if self.active_scrape:
            # Try to read the latest log
            log_content = []
            try:
                with open('web_archiver.log', 'r') as f:
                    # Get last 50 lines
                    log_content = f.readlines()[-50:]
            except:
                pass
            
            return web.json_response({
                'status': 'running',
                'pid': self.active_scrape.pid,
                'log': log_content
            })
        
        return web.json_response({
            'status': 'idle'
        })
    
    async def stop_scrape(self, request):
        """Stop active scrape"""
        if self.active_scrape:
            self.active_scrape.terminate()
            await self.active_scrape.wait()
            self.active_scrape = None
            
            return web.json_response({
                'status': 'stopped'
            })
        
        return web.json_response({
            'error': 'No active scrape'
        }, status=400)
    
    async def get_scrape_config(self, request):
        """Get current scraping configuration"""
        load_dotenv()
        
        config = {
            'START_URL': os.getenv('START_URL', 'https://example.com'),
            'MAX_WORKERS': int(os.getenv('MAX_WORKERS', 10)),
            'MAX_DEPTH': int(os.getenv('MAX_DEPTH', 3)),
            'MAX_PAGES': int(os.getenv('MAX_PAGES', 100)),
            'PAGES_PER_DOMAIN': int(os.getenv('PAGES_PER_DOMAIN', 50)),
            'IMAGE_QUALITY': int(os.getenv('IMAGE_QUALITY', 85)),
            'MAX_IMAGE_WIDTH': int(os.getenv('MAX_IMAGE_WIDTH', 1920)),
            'COMPRESSION_LEVEL': int(os.getenv('COMPRESSION_LEVEL', 19)),
            'SKIP_ASSETS': os.getenv('SKIP_ASSETS', 'false').lower() == 'true',
            'RESPECT_ROBOTS_TXT': os.getenv('RESPECT_ROBOTS_TXT', 'true').lower() == 'true',
            'REQUEST_DELAY': float(os.getenv('REQUEST_DELAY', 0.5))
        }
        
        return web.json_response(config)
    
    async def get_runs(self, request):
        """Get all scraping runs"""
        runs = []
        
        if self.scraped_data_dir.exists():
            for run_dir in sorted(self.scraped_data_dir.iterdir(), reverse=True):
                if run_dir.is_dir() and run_dir.name.replace('_', '').isdigit():
                    metadata_file = run_dir / 'metadata.json'
                    if metadata_file.exists():
                        with open(metadata_file, 'r') as f:
                            metadata = json.load(f)
                            
                        runs.append({
                            'id': run_dir.name,
                            'timestamp': metadata.get('timestamp', run_dir.name),
                            'start_url': metadata.get('start_url', ''),
                            'total_pages': metadata.get('total_pages', 0),
                            'pages_scraped': metadata.get('pages_scraped', 0),
                            'stats': metadata.get('stats', {})
                        })
        
        return web.json_response(runs)
    
    async def get_run_details(self, request):
        """Get details for a specific run"""
        run_id = request.match_info['run_id']
        run_dir = self.scraped_data_dir / run_id
        
        if not run_dir.exists():
            return web.json_response({'error': 'Run not found'}, status=404)
        
        metadata_file = run_dir / 'metadata.json'
        if not metadata_file.exists():
            return web.json_response({'error': 'Metadata not found'}, status=404)
        
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        
        # Check for compression report
        compression_report = None
        for archive_file in self.archives_dir.glob('compression_report.json'):
            with open(archive_file, 'r') as f:
                report = json.load(f)
                if run_id in report.get('source_directory', ''):
                    compression_report = report
                    break
        
        return web.json_response({
            'id': run_id,
            'metadata': metadata,
            'compression_report': compression_report
        })
    
    async def get_run_pages(self, request):
        """Get all pages for a specific run"""
        run_id = request.match_info['run_id']
        run_dir = self.scraped_data_dir / run_id
        
        # Pagination parameters
        page = int(request.query.get('page', 1))
        per_page = int(request.query.get('per_page', 50))
        search = request.query.get('search', '').lower()
        domain_filter = request.query.get('domain', '')
        
        metadata_file = run_dir / 'metadata.json'
        if not metadata_file.exists():
            return web.json_response({'error': 'Metadata not found'}, status=404)
        
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        
        # Get pages and apply filters
        pages = metadata.get('pages', {})
        filtered_pages = []
        
        for url, page_data in pages.items():
            # Apply search filter
            if search and search not in url.lower() and search not in page_data.get('domain', '').lower():
                continue
            
            # Apply domain filter
            if domain_filter and page_data.get('domain', '') != domain_filter:
                continue
                
            filtered_pages.append({
                'url': url,
                'hash': self.get_url_hash(url),
                **page_data
            })
        
        # Sort by timestamp
        filtered_pages.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        
        # Pagination
        total = len(filtered_pages)
        start = (page - 1) * per_page
        end = start + per_page
        
        return web.json_response({
            'pages': filtered_pages[start:end],
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total,
                'total_pages': (total + per_page - 1) // per_page
            }
        })
    
    async def get_page_content(self, request):
        """Get content of a specific page"""
        run_id = request.match_info['run_id']
        page_hash = request.match_info['page_hash']
        
        run_dir = self.scraped_data_dir / run_id
        
        # Try to find the file in html or assets directory
        for subdir in ['html', 'assets']:
            for ext in ['.html', '.css', '.js', '.json', '.txt']:
                filepath = run_dir / subdir / f"{page_hash}{ext}"
                if filepath.exists():
                    async with aiofiles.open(filepath, 'r', encoding='utf-8') as f:
                        content = await f.read()
                    
                    return web.json_response({
                        'content': content,
                        'type': ext[1:],
                        'size': len(content)
                    })
        
        return web.json_response({'error': 'Page content not found'}, status=404)
    
    async def get_run_stats(self, request):
        """Get statistics for a specific run"""
        run_id = request.match_info['run_id']
        run_dir = self.scraped_data_dir / run_id
        
        metadata_file = run_dir / 'metadata.json'
        if not metadata_file.exists():
            return web.json_response({'error': 'Metadata not found'}, status=404)
        
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        
        stats = metadata.get('stats', {})
        domain_counts = metadata.get('domain_counts', {})
        
        # Calculate additional stats
        pages = metadata.get('pages', {})
        content_types = {}
        depths = {}
        
        for page_data in pages.values():
            # Content type distribution
            ct = page_data.get('content_type', 'unknown')
            ct_simple = ct.split(';')[0].strip()
            content_types[ct_simple] = content_types.get(ct_simple, 0) + 1
            
            # Depth distribution
            depth = page_data.get('depth', 0)
            depths[str(depth)] = depths.get(str(depth), 0) + 1
        
        return web.json_response({
            'basic_stats': stats,
            'domain_distribution': domain_counts,
            'content_types': content_types,
            'depth_distribution': depths,
            'top_domains': sorted(domain_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        })
    
    async def get_archives(self, request):
        """Get all archives"""
        archives = []
        
        if self.archives_dir.exists():
            for archive_file in sorted(self.archives_dir.glob('*.tar.*'), reverse=True):
                archives.append({
                    'name': archive_file.name,
                    'size': archive_file.stat().st_size,
                    'created': datetime.fromtimestamp(archive_file.stat().st_mtime).isoformat()
                })
        
        return web.json_response(archives)
    
    def get_url_hash(self, url):
        """Generate hash for URL"""
        import hashlib
        return hashlib.md5(url.encode()).hexdigest()
    
    async def start(self):
        """Start the server"""
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, 'localhost', self.port)
        await site.start()
        
        logger.info(f"Server started at http://localhost:{self.port}")
        logger.info("Press Ctrl+C to stop")
        
        # Keep the server running
        try:
            await asyncio.Event().wait()
        except KeyboardInterrupt:
            logger.info("Server stopped")

async def main():
    server = WebArchiveServer()
    await server.start()

if __name__ == '__main__':
    asyncio.run(main())