"""
Guncad Index Scraper
Search + Deep Scrape with SQLite & JSON
"""

import asyncio
import time
from argparse import Namespace
from pathlib import Path
from typing import Dict, List, Tuple

import aiohttp  # type: ignore[import]
import logging
import random
import sqlite3
import json
from bs4 import BeautifulSoup
from urllib.parse import parse_qs, unquote

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


def configure_logging(level: int) -> None:
    logging.getLogger().setLevel(level)
    logger.setLevel(level)


class GuncadScraper:
    def __init__(self, start_page: int = 1, end_page: int = 233,
                 concurrent: int = 5, concurrent_deep: int = 3,
                 delay_range: tuple = (1, 2), db_path: str = "guncad.db"):
        self.base_url = "https://guncadindex.com/search"
        self.start_page = start_page
        self.end_page = end_page
        self.concurrent = concurrent
        self.concurrent_deep = concurrent_deep
        self.delay_range = delay_range

        self.user_agent = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                           'AppleWebKit/537.36 (KHTML, like Gecko) '
                           'Chrome/91.0.4472.124 Safari/537.36')

        self.links: List[Dict] = []
        self.failed_pages: List[int] = []
        self.failed_deep: List[str] = []
        self.db_path = db_path
        self.init_db()

    def get_headers(self) -> Dict:
        return {'User-Agent': self.user_agent, 'Accept': 'text/html'}

    async def fetch(self, session: aiohttp.ClientSession, url: str) -> str:
        try:
            async with session.get(url, headers=self.get_headers()) as r:
                if r.status == 200:
                    logger.info(f"✓ {url[:50]}...")
                    return await r.text()
                logger.error(f"✗ HTTP {r.status}: {url[:50]}...")
                return ""
        except Exception as e:
            logger.error(f"✗ Error: {e}")
            return ""

    def extract_links(self, html: str, page_num: int) -> None:
        if not html:
            return

        for div in BeautifulSoup(html, 'html.parser').find_all('div', class_='grid-view-max'):
            a = div.find('a', href=True)
            if not a:
                continue

            href = a['href']
            if href.startswith('/'):
                href = f"https://guncadindex.com{href}"

            title = (div.find(['h2', 'h3', 'h4'], class_=lambda x: x and 'title' in x.lower()) or
                     a.get('title') or a).get_text(strip=True)

            if title:
                self.links.append({'Page': str(page_num), 'Title': title[:100], 'Link': href})

    async def scan_search(self, session: aiohttp.ClientSession, page: int) -> None:
        await asyncio.sleep(random.uniform(*self.delay_range))
        html = await self.fetch(session, f"{self.base_url}?page={page}")
        self.extract_links(html, page)

    async def stage1_search(self) -> None:
        connector = aiohttp.TCPConnector(limit=self.concurrent, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [self.scan_search(session, p) for p in range(self.start_page, self.end_page + 1)]
            await asyncio.gather(*tasks)
            if self.links:
                self.save_links()

    def init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('PRAGMA foreign_keys = ON')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS links (
                    id INTEGER PRIMARY KEY, page INTEGER, title TEXT,
                    link TEXT UNIQUE, scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
            ''')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_page ON links(page)')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS details (
                    id INTEGER PRIMARY KEY, link_id INTEGER UNIQUE,
                    description TEXT, scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (link_id) REFERENCES links(id) ON DELETE CASCADE)
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS exturl (
                    id INTEGER PRIMARY KEY, link_id INTEGER,
                    external_url TEXT, source_href TEXT, link_text TEXT,
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (link_id) REFERENCES links(id) ON DELETE CASCADE)
            ''')
        logger.info(f"📦 DB: {self.db_path}")

    def save_links(self) -> int:
        inserted = 0
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for item in self.links:
                try:
                    cursor.execute('INSERT OR IGNORE INTO links (page, title, link) VALUES (?, ?, ?)',
                                   (int(item['Page']), item['Title'], item['Link']))
                    if cursor.rowcount > 0:
                        inserted += 1
                except sqlite3.Error as e:
                    logger.warning(f"Insert failed: {e}")
            conn.commit()
        logger.info(f"💾 Saved {inserted} new links")
        return inserted

    def get_unscraped(self) -> List[Tuple[int, str]]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT l.id, l.link FROM links l LEFT JOIN details d ON l.id = d.link_id WHERE d.link_id IS NULL')
            return cursor.fetchall()

    async def scrape_detail(self, session: aiohttp.ClientSession, link_id: int, url: str) -> None:
        await asyncio.sleep(random.uniform(*self.delay_range))
        html = await self.fetch(session, url)
        if not html:
            self.failed_deep.append(url)
            return

        soup = BeautifulSoup(html, 'html.parser')

        # Extract description
        desc = soup.select_one('div.description-container span.description-text.textbox.md')
        description = desc.get_text(strip=True) if desc else ""

        # Extract external URLs
        ext_links = []
        for a in soup.find_all('a', href=lambda h: h and '/out/?u=' in h):
            u = parse_qs(a['href'].split('?')[1]).get('u', [None])[0] if '?' in a['href'] else None
            if u:
                ext_links.append({
                    'external_url': unquote(u),
                    'source_href': a['href'],
                    'link_text': a.get_text(strip=True)[:50]
                })

        # Save to DB
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            if description:
                cursor.execute('INSERT OR REPLACE INTO details (link_id, description) VALUES (?, ?)',
                               (link_id, description))
            for ext in ext_links:
                cursor.execute('INSERT INTO exturl (link_id, external_url, source_href, link_text) VALUES (?, ?, ?, ?)',
                               (link_id, ext['external_url'], ext['source_href'], ext['link_text']))
            conn.commit()

    async def stage2_deep(self) -> None:
        unscraped = self.get_unscraped()
        if not unscraped:
            logger.info("ℹ️  No unscraped links")
            return

        logger.info(f"🔍 Deep scraping {len(unscraped)} links...")
        connector = aiohttp.TCPConnector(limit=self.concurrent_deep, ssl=False)

        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [self.scrape_detail(session, link_id, url) for link_id, url in unscraped]
            await asyncio.gather(*tasks)

            if self.failed_deep:
                logger.warning(f"⚠️  Failed: {len(self.failed_deep)} detail pages")

    def export_json(self, json_path: str) -> Path:
        """Export all data (links + details + exturls) to JSON"""
        export_data = {
            "metadata": {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "search_pages": f"{self.start_page}-{self.end_page}",
                "mode": "search+deep" if self.failed_deep else "search"
            },
            "items": []
        }

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT l.id, l.page, l.title, l.link, d.description
                FROM links l
                LEFT JOIN details d ON l.id = d.link_id
                ORDER BY l.page, l.id
            ''')

            for link_id, page, title, link, desc in cursor.fetchall():
                item = {
                    "Page": page,
                    "Title": title,
                    "Link": link,
                    "Description": desc or ""
                }

                # Get external URLs for this link
                cursor.execute('SELECT external_url, link_text FROM exturl WHERE link_id = ?', (link_id,))
                ext_urls = cursor.fetchall()
                if ext_urls:
                    item["ExternalURLs"] = [{"url": url, "text": text} for url, text in ext_urls]

                export_data["items"].append(item)

        output = Path(json_path)
        with open(output, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)

        logger.info(f"📄 JSON exported with {len(export_data['items'])} items")
        return output

    def get_stats(self) -> Dict:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*), COUNT(DISTINCT page) FROM links')
            total_links, pages = cursor.fetchone()

            cursor.execute('SELECT COUNT(DISTINCT link_id) FROM details')
            details = cursor.fetchone()[0]

            cursor.execute('SELECT COUNT(DISTINCT link_id), COUNT(*) FROM exturl')
            ext_links, total_ext = cursor.fetchone()

            return {
                'total_links': total_links,
                'pages_with_links': pages,
                'links_with_details': details,
                'links_with_ext_urls': ext_links,
                'total_ext_urls': total_ext
            }


async def _run_scraper(args: Namespace) -> Dict:
    if args.fast:
        args.c = 10
        delay_range = (0.5, 1.5)
    else:
        delay_range = (1, 2)

    print(f"\n🚀 Guncad Scraper v2.0")
    print(f"Mode: {'Search + Deep Scrape' if args.deep else 'Search Only'}")
    print(f"Pages: {args.s}-{args.e} | DB: {args.db}")
    if args.o:
        print(f"JSON: {args.o}")
    print(f"Speed: Search={args.c}, Deep={args.cd}, Delay={delay_range[0]}-{delay_range[1]}s\n")

    scraper = GuncadScraper(
        start_page=args.s,
        end_page=args.e,
        concurrent=args.c,
        concurrent_deep=args.cd,
        delay_range=delay_range,
        db_path=args.db
    )

    # Stage 1
    start = time.time()
    await scraper.stage1_search()
    search_time = time.time() - start

    # Stage 2
    deep_time = 0
    if args.deep:
        deep_start = time.time()
        await scraper.stage2_deep()
        deep_time = time.time() - deep_start

    # Export
    json_file = None
    if args.o:
        json_file = scraper.export_json(args.o)

    # Stats
    stats = scraper.get_stats()
    total = time.time() - start

    # Summary
    print("\n" + "="*60)
    print(f"✅ COMPLETE in {total:.1f}s (Search: {search_time:.1f}s, Deep: {deep_time:.1f}s)")
    print(f"📊 Items: {stats['total_links']} | Pages: {stats['pages_with_links']}")
    print(f"📝 Descriptions: {stats['links_with_details']} | URLs: {stats['total_ext_urls']}")
    print(f"💾 DB: {Path(args.db).absolute()}")
    if json_file:
        print(f"📄 JSON: {json_file.absolute()}")
    print("="*60)

    return {
        "stats": stats,
        "json_file": str(json_file.absolute()) if json_file else None,
        "db_path": str(Path(args.db).absolute()),
        "failed_details": list(scraper.failed_deep),
    }


def run_scraper(args: Namespace) -> Dict:
    """
    Execute the scraper with a parsed argparse Namespace.
    """
    return asyncio.run(_run_scraper(args))