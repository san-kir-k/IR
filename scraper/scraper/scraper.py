import asyncio
import coloredlogs
import logging

from aiolimiter import AsyncLimiter
from httpx import AsyncClient, Response, codes
from bs4 import BeautifulSoup
from robots import RobotsParser
from typing import Tuple, Set
from functools import partial, wraps

from utils import Settings
from bucket_queue import BucketQueue
from db import save_documents


class Scraper:
    logger = logging.getLogger(__name__)
    
    def __init__(self, settings: Settings):
        self.settings: Settings = settings
        Scraper.logger.setLevel(level=self.settings.log_level)
        coloredlogs.install(level=Scraper.logger.level)

        self.robots = RobotsParser.from_uri(uri=f"{self.settings.url_base}/robots.txt")
        if self.settings.rps > 50:
            Scraper.logger.warning("Your rps(%s) too large", self.settings.rps)
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
        Scraper.logger.info("Discarded %s refs from current batch of documents", len(urls) - len(filtered))
        return filtered

    def _find_hrefs(self, sources: Tuple) -> Set[str]:
        result: Set[str] = set()
        for s in sources:
            if not s:
                continue
            soup = BeautifulSoup(s.text, features="html.parser")
            result |= {a['href'] for a in soup.find_all('a', href=True)}
        Scraper.logger.info("Got %s refs from current batch of documents", len(result))
        return result

    @staticmethod
    def _handle_response(task):
        @wraps(task)
        async def wrapper(self, url: str, client: AsyncClient):
            retries: int = 0
            max_retires: int = 5
            response: Response = await task(self, url, client)

            while ((response.status_code == codes.TOO_MANY_REQUESTS or response.status_code == codes.SERVICE_UNAVAILABLE)
                   and retries <= max_retires):
                Scraper.logger.warning("Too many request for url: %s, retry: %s", url, retries)
                await asyncio.sleep(1)
                response = await task(self, url, url, client)
                retries += 1

            if retries > max_retires:
                Scraper.logger.error("Can't scrape url: %s, max retries was reached", url)
                return None

            if codes.is_redirect(response.status_code):
                Scraper.logger.warning("Redirect on url: %s", url)
                response = await task(self, response.headers['Location'], client)

            if response.status_code != codes.OK:
                Scraper.logger.error("Response error on url: %s, code %s", url, response.status_code)
                return None

            return response

        return wrapper

    @_handle_response
    async def _scrape(self, url: str, client: AsyncClient) -> Response:
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

                if self.settings.is_file_output:
                    for i, html in enumerate(results):
                        if not html.text:
                            continue

                        with open(f'./{self.settings.out_dir}/{i + self.scraped_count}.txt', 'w') as f:
                            f.write(html.text)
                else:
                    await save_documents(results, Scraper.logger)

                self.scraped_count += len(results) - results.count(None)
                Scraper.logger.info("Scraped: %s", self.scraped_count)

                if self.scraped_count >= self.settings.max_scraped_count:
                    break
