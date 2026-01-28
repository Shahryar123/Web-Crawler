import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urldefrag
import time
from collections import deque
import validators
import re
from datetime import datetime

class WebCrawler:
    def __init__(self, start_url, max_depth=3, max_urls=1000, socketio=None, session_id=None):
        self.start_url = self.normalize_url(start_url)
        self.max_depth = max_depth
        self.max_urls = max_urls
        self.socketio = socketio
        self.session_id = session_id
        
        self.valid_urls = set()
        self.visited_urls = set()
        self.failed_urls = set()
        self.domain = urlparse(self.start_url).netloc
        
        self.start_time = None
        self.end_time = None
        
        # Queue: (url, depth)
        self.queue = deque([(self.start_url, 0)])
        
    def normalize_url(self, url):
        """Normalize URL by removing fragments and trailing slashes"""
        url, _ = urldefrag(url)
        if url.endswith('/') and url.count('/') > 2:
            url = url.rstrip('/')
        return url
    
    def is_valid_url(self, url):
        """Check if URL is valid and belongs to the same domain"""
        if not validators.url(url):
            return False
        
        parsed = urlparse(url)
        
        # Check if same domain
        if parsed.netloc != self.domain:
            return False
        
        # Exclude common non-HTML resources
        excluded_extensions = [
            '.jpg', '.jpeg', '.png', '.gif', '.pdf', '.zip',
            '.mp4', '.mp3', '.avi', '.css', '.js', '.ico',
            '.svg', '.woff', '.woff2', '.ttf', '.eot'
        ]
        
        if any(parsed.path.lower().endswith(ext) for ext in excluded_extensions):
            return False
        
        return True
    
    async def fetch_url(self, session, url, depth):
        """Fetch a single URL and extract links"""
        if url in self.visited_urls or len(self.valid_urls) >= self.max_urls:
            return []
        
        self.visited_urls.add(url)
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            async with session.get(url, timeout=10, headers=headers, allow_redirects=True) as response:
                if response.status == 200:
                    content_type = response.headers.get('Content-Type', '')
                    
                    if 'text/html' in content_type:
                        self.valid_urls.add(url)
                        
                        # Emit progress
                        self.emit_progress()
                        
                        # Only parse HTML for links if we haven't reached max depth
                        if depth < self.max_depth:
                            html = await response.text()
                            return self.extract_links(html, url)
                else:
                    self.failed_urls.add(url)
        
        except Exception as e:
            self.failed_urls.add(url)
            print(f"Error fetching {url}: {str(e)}")
        
        return []
    
    def extract_links(self, html, base_url):
        """Extract all links from HTML"""
        links = []
        
        try:
            soup = BeautifulSoup(html, 'lxml')
            
            for tag in soup.find_all(['a', 'link']):
                href = tag.get('href')
                
                if href:
                    # Convert relative URLs to absolute
                    absolute_url = urljoin(base_url, href)
                    normalized_url = self.normalize_url(absolute_url)
                    
                    if self.is_valid_url(normalized_url):
                        links.append(normalized_url)
        
        except Exception as e:
            print(f"Error parsing HTML from {base_url}: {str(e)}")
        
        return links
    
    async def crawl_async(self):
        """Main async crawling function"""
        self.start_time = time.time()
        
        connector = aiohttp.TCPConnector(limit=50, limit_per_host=10)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            while self.queue and len(self.valid_urls) < self.max_urls:
                # Process batch of URLs
                batch_size = min(20, len(self.queue))
                tasks = []
                
                for _ in range(batch_size):
                    if not self.queue:
                        break
                    
                    url, depth = self.queue.popleft()
                    
                    if url not in self.visited_urls:
                        tasks.append(self.fetch_url(session, url, depth))
                
                # Wait for all tasks in batch to complete
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Add new URLs to queue
                for result in results:
                    if isinstance(result, list):
                        for new_url in result:
                            if new_url not in self.visited_urls:
                                # Get the depth for this URL
                                current_depth = 0
                                for queued_url, d in list(self.queue):
                                    if queued_url == new_url:
                                        current_depth = d
                                        break
                                
                                # Add with incremented depth
                                new_depth = current_depth + 1
                                if new_depth <= self.max_depth:
                                    self.queue.append((new_url, new_depth))
        
        self.end_time = time.time()
        self.emit_complete()
    
    def crawl(self):
        """Start the crawling process"""
        asyncio.run(self.crawl_async())
    
    def emit_progress(self):
        """Emit progress to frontend via SocketIO"""
        if self.socketio and self.session_id:
            elapsed_time = time.time() - self.start_time
            
            progress_data = {
                'session_id': self.session_id,
                'total_found': len(self.valid_urls),
                'total_visited': len(self.visited_urls),
                'total_failed': len(self.failed_urls),
                'queue_size': len(self.queue),
                'elapsed_time': round(elapsed_time, 2),
                'urls_per_second': round(len(self.visited_urls) / elapsed_time, 2) if elapsed_time > 0 else 0,
                'status': 'crawling'
            }
            
            self.socketio.emit('crawl_progress', progress_data)
    
    def emit_complete(self):
        """Emit completion message"""
        if self.socketio and self.session_id:
            total_time = self.end_time - self.start_time
            
            complete_data = {
                'session_id': self.session_id,
                'total_found': len(self.valid_urls),
                'total_visited': len(self.visited_urls),
                'total_failed': len(self.failed_urls),
                'total_time': round(total_time, 2),
                'status': 'complete'
            }
            
            self.socketio.emit('crawl_complete', complete_data)
    
    def generate_sitemap(self):
        """Generate XML sitemap"""
        xml_lines = ['<?xml version="1.0" encoding="UTF-8"?>']
        xml_lines.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')
        
        for url in sorted(self.valid_urls):
            xml_lines.append('  <url>')
            xml_lines.append(f'    <loc>{url}</loc>')
            xml_lines.append(f'    <lastmod>{datetime.now().strftime("%Y-%m-%d")}</lastmod>')
            xml_lines.append('    <changefreq>weekly</changefreq>')
            xml_lines.append('    <priority>0.5</priority>')
            xml_lines.append('  </url>')
        
        xml_lines.append('</urlset>')
        
        return '\n'.join(xml_lines)