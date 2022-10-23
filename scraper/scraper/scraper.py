import asyncio
import coloredlogs
import logging

from aiolimiter import AsyncLimiter
from httpx import AsyncClient
from bs4 import BeautifulSoup
from robots import RobotsParser
from typing import Tuple, Set
from functools import partial

from utils import Settings
from bucket_queue import BucketQueue


class Scraper:
    def __init__(self, settings: Settings):
        self.settings: Settings = settings
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(level=self.settings.log_level)
        coloredlogs.install(level=self.logger.level)

        self.robots = RobotsParser.from_uri(uri=f"{self.settings.url_base}/robots.txt")
        if self.settings.rps > 50:
            self.logger.warning("Your rps(%s) too large", self.settings.rps)
        self.throttler = AsyncLimiter(max_rate=self.settings.rps, time_period=1)

        self.queue = BucketQueue(settings=self.settings)
        self.queue.append(self.settings.start_url)
        self.visited = {self.settings.start_url}
        self.scraped_count = 0

    def _filter_urls(self, urls: Set[str]) -> Set[str]:
        filtered: Set[str] = {f'{self.settings.url_base}{url}' for url in urls
                              if url.startswith('/wiki/')
                              and self.robots.can_fetch("*", url)
                              and ":" not in url
                              and f'{self.settings.url_base}{url}' not in self.visited}
        self.logger.info("Discarded %s refs from current batch of documents", len(urls) - len(filtered))
        return filtered

    def _find_hrefs(self, sources: Tuple) -> Set[str]:
        result: Set[str] = set()
        for s in sources:
            soup = BeautifulSoup(s.text, features="html.parser")
            result |= {a['href'] for a in soup.find_all('a', href=True)}
        self.logger.info("Got %s refs from current batch of documents", len(result))
        return result

    async def _scrape(self, url: str, client: AsyncClient):
        async with self.throttler:
            return await client.get(url)

    async def run(self):
        async with AsyncClient() as client:
            task = partial(self._scrape, client=client)
            self.queue.set_task(task)
            for tasks in self.queue:
                results: Tuple = await asyncio.gather(*tasks)
                found_refs: Set[str] = self._filter_urls(self._find_hrefs(results))
                self.queue.extend(found_refs)
                self.visited |= found_refs

                for i, html in enumerate(results):
                    if not html.text:
                        continue

                    with open(f'./{self.settings.out_dir}/{i + self.scraped_count}.txt', 'w') as f:
                        f.write(html.text)

                self.scraped_count += len(results)
                self.logger.info("Scraped: %s", self.scraped_count)

                if self.scraped_count >= self.settings.max_scraped_count:
                    break
