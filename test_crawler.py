import asyncio
import pytest

try:
    from .crawler import DistributedMapReduceCrawler, SQLiteTaskQueue, clean_html_content, extract_links
    from .models import CrawlTask, ExtractedDocument, TaskStatus
except ImportError:
    from crawler import DistributedMapReduceCrawler, SQLiteTaskQueue, clean_html_content, extract_links
    from models import CrawlTask, ExtractedDocument, TaskStatus


def test_clean_html_content():
    raw = """
    <html>
        <head><script>alert('bad');</script></head>
        <body>
            <h1>Title Header</h1>
            <p>Body paragraph text.</p>
        </body>
    </html>
    """
    cleaned = clean_html_content(raw)
    assert "alert" not in cleaned
    assert "Title Header" in cleaned
    assert "Body paragraph text." in cleaned


def test_extract_links():
    raw = """
    <a href="https://example.com/page1">Link 1</a>
    <a href="/internal_page">Internal</a>
    """
    links = extract_links(raw, "https://example.com")
    assert "https://example.com/page1" in links
    assert "https://example.com/internal_page" in links


def test_sqlite_task_queue_lifecycle():
    queue = SQLiteTaskQueue(":memory:")
    task = CrawlTask(url="https://example.com", domain="example.com")
    assert queue.push_task(task) is True
    assert queue.push_task(task) is False  # Duplicate ignored

    claimed = queue.claim_next_task()
    assert claimed is not None
    assert claimed.url == "https://example.com"
    assert claimed.status == TaskStatus.IN_PROGRESS

    doc = ExtractedDocument(
        url="https://example.com",
        domain="example.com",
        title="Example",
        content_text="Some text content",
        content_hash="abc",
        word_count=3,
        discovered_links=[],
    )
    queue.mark_completed("https://example.com", doc)

    stats = queue.get_stats()
    assert stats.total_completed == 1
    assert stats.total_words_extracted == 3


@pytest.mark.asyncio
async def test_crawler_end_to_end():
    crawler = DistributedMapReduceCrawler(max_depth=1)
    crawler.seed(["https://node1.example.org", "https://node2.example.org"])

    stats = await crawler.run(max_tasks=5)
    assert stats.total_completed >= 2
    assert stats.total_words_extracted > 0


def test_extract_links_preserves_http_scheme():
    raw = '<a href="/docs/intro">Intro</a>'
    links = extract_links(raw, "http://plain.example.com")
    assert links == ["http://plain.example.com/docs/intro"]


@pytest.mark.asyncio
async def test_crawler_multiline_title_and_concurrency():
    active = 0
    peak_active = 0

    async def slow_fetcher(url: str) -> str:
        nonlocal active, peak_active
        active += 1
        peak_active = max(peak_active, active)
        await asyncio.sleep(0.03)
        active -= 1
        return "<html><head><title>\n  Multiline\n  Title\n</title></head><body>Hello world</body></html>"

    crawler = DistributedMapReduceCrawler(max_concurrency=3, max_depth=0, fetcher=slow_fetcher)
    crawler.seed(["https://a.example.org", "https://b.example.org", "https://c.example.org"])
    stats = await crawler.run(max_tasks=3)
    assert stats.total_completed == 3
    assert peak_active >= 2

    row = crawler.queue.conn.execute("SELECT title FROM documents WHERE url = 'https://a.example.org'").fetchone()
    assert "Multiline" in row["title"] and "Title" in row["title"]
