"""Distributed MapReduce Crawler."""
try:
    from .models import CrawlStats, CrawlTask, ExtractedDocument, TaskStatus
    from .crawler import DistributedMapReduceCrawler, SQLiteTaskQueue, clean_html_content, extract_links
except (ImportError, ValueError):
    from models import CrawlStats, CrawlTask, ExtractedDocument, TaskStatus
    from crawler import DistributedMapReduceCrawler, SQLiteTaskQueue, clean_html_content, extract_links
