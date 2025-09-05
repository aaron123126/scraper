#!/usr/bin/env python3

import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path
import logging
from dotenv import load_dotenv
from scraper import WebScraper
from compressor import WebCompressor
from utils import ensure_directories, save_json

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('web_archiver.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

async def scrape_and_compress():
    """Main workflow: scrape and compress"""
    
    # Load configuration from environment
    start_url = os.getenv('START_URL', 'https://example.com')
    max_workers = int(os.getenv('MAX_WORKERS', 10))
    max_depth = int(os.getenv('MAX_DEPTH', 3))
    max_pages = int(os.getenv('MAX_PAGES', 100))
    pages_per_domain = int(os.getenv('PAGES_PER_DOMAIN', 50))
    output_dir = os.getenv('OUTPUT_DIR', './scraped_data')
    archive_dir = os.getenv('ARCHIVE_DIR', './archives')
    image_quality = int(os.getenv('IMAGE_QUALITY', 85))
    max_image_width = int(os.getenv('MAX_IMAGE_WIDTH', 1920))
    compression_level = int(os.getenv('COMPRESSION_LEVEL', 19))
    skip_assets = os.getenv('SKIP_ASSETS', 'false').lower() == 'true'
    respect_robots = os.getenv('RESPECT_ROBOTS_TXT', 'true').lower() == 'true'
    request_delay = float(os.getenv('REQUEST_DELAY', 0.5))
    
    logger.info("=" * 60)
    logger.info("Web Archiver - Scraper & Compressor")
    logger.info("=" * 60)
    logger.info(f"Start URL: {start_url}")
    logger.info(f"Max Depth: {max_depth}")
    logger.info(f"Max Pages: {max_pages}")
    logger.info(f"Pages per Domain: {pages_per_domain}")
    logger.info(f"Max Workers: {max_workers}")
    logger.info(f"Skip Assets: {skip_assets}")
    logger.info(f"Respect Robots.txt: {respect_robots}")
    logger.info(f"Request Delay: {request_delay}s")
    logger.info(f"Output Directory: {output_dir}")
    logger.info(f"Archive Directory: {archive_dir}")
    logger.info("=" * 60)
    
    # Create timestamp for this run
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    run_output_dir = f"{output_dir}/{timestamp}"
    
    # Ensure directories exist
    ensure_directories(run_output_dir, archive_dir)
    
    try:
        # Phase 1: Scraping
        logger.info("\n📥 PHASE 1: Web Scraping")
        logger.info("-" * 40)
        
        scraper = WebScraper(
            start_url=start_url,
            output_dir=run_output_dir,
            max_workers=max_workers,
            max_depth=max_depth,
            max_pages=max_pages,
            pages_per_domain=pages_per_domain,
            respect_robots=respect_robots,
            request_delay=request_delay,
            skip_assets=skip_assets
        )
        
        scraped_data = await scraper.run()
        
        if not scraped_data:
            logger.warning("No data scraped. Exiting.")
            return
        
        # Save URLs to JSON
        urls_file = f"{run_output_dir}/scraped_urls.json"
        save_json({
            'start_url': start_url,
            'timestamp': timestamp,
            'total_urls': len(scraped_data),
            'max_pages_limit': max_pages,
            'pages_per_domain_limit': pages_per_domain,
            'urls': list(scraped_data.keys())
        }, urls_file)
        
        logger.info(f"✅ Scraping complete. {len(scraped_data)} pages saved")
        logger.info(f"📄 URLs saved to: {urls_file}")
        
        # Ask user if they want to continue with compression
        if len(scraped_data) >= max_pages * 0.9:  # If we hit near the limit
            logger.info(f"\n⚠️  Page limit reached ({len(scraped_data)}/{max_pages})")
            user_input = input("Continue with compression? (y/n): ").lower()
            if user_input != 'y':
                logger.info("Compression skipped by user")
                return
        
        # Phase 2: Compression
        logger.info("\n🗜️ PHASE 2: Optimization & Compression")
        logger.info("-" * 40)
        
        compressor = WebCompressor(
            source_dir=run_output_dir,
            archive_dir=archive_dir,
            compression_level=compression_level
        )
        
        # Set optimizer parameters
        compressor.optimizer.image_quality = image_quality
        compressor.optimizer.max_image_width = max_image_width
        
        compression_report = await compressor.compress()
        
        logger.info("\n" + "=" * 60)
        logger.info("✅ ARCHIVING COMPLETE")
        logger.info("=" * 60)
        logger.info(f"📦 Archive: {compression_report['archive_path']}")
        logger.info(f"📊 Original Size: {compression_report['original_size']:,} bytes")
        logger.info(f"📊 Compressed Size: {compression_report['compressed_size']:,} bytes")
        logger.info(f"📊 Compression Ratio: {compression_report['compression_ratio']}")
        logger.info("=" * 60)
        
        # Final summary
        summary = {
            'run_timestamp': timestamp,
            'start_url': start_url,
            'pages_scraped': len(scraped_data),
            'max_pages_limit': max_pages,
            'reached_limit': len(scraped_data) >= max_pages,
            'urls_file': urls_file,
            'archive_path': compression_report['archive_path'],
            'compression_ratio': compression_report['compression_ratio'],
            'optimization_savings': compression_report['optimization_stats']['total_saved']
        }
        
        summary_file = f"{archive_dir}/run_summary_{timestamp}.json"
        save_json(summary, summary_file)
        
        logger.info(f"\n📋 Summary saved to: {summary_file}")
        
    except KeyboardInterrupt:
        logger.info("\n⚠️ Process interrupted by user")
    except Exception as e:
        logger.error(f"❌ Error in main workflow: {e}", exc_info=True)
        raise

def main():
    """Entry point"""
    try:
        # Check Python version
        if sys.version_info < (3, 7):
            print("Python 3.7+ required")
            sys.exit(1)
        
        # Run the async workflow
        asyncio.run(scrape_and_compress())
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()