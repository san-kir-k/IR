import asyncio
import coloredlogs
import logging

from aiolimiter import AsyncLimiter
from httpx import AsyncClient, Response, codes
from bs4 import BeautifulSoup
from robots import RobotsParser
from typing import Tuple, Set, List
from functools import partial, wraps

from utils import Settings
from bucket_queue import BucketQueue
from db import save_documents, save_words, save_bigrams,\
    load_visited_urls, dump_visited_urls, load_pending_urls, dump_pending_urls
from text_enrich import enrich_text, get_text


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

    async def _load_scraper_state(self):
        loaded_visited_urls: Set[str] = await load_visited_urls(Scraper.logger)
        if loaded_visited_urls:
            self.visited = loaded_visited_urls
        else:
            self.visited = {self.settings.start_url}
            await dump_visited_urls({self.settings.start_url}, Scraper.logger)

        self.queue = BucketQueue(settings=self.settings)
        loaded_pending_urls: Set[str] = await load_pending_urls(Scraper.logger)
        if loaded_pending_urls:
            self.queue.extend(loaded_pending_urls)
        else:
            self.queue.extend(self.visited)

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
        await self._load_scraper_state()
        async with AsyncClient() as client:
            task = partial(self._scrape, client=client)
            self.queue.set_task(task)
            for tasks in self.queue:
                results: Tuple = await asyncio.gather(*tasks)

                if len(self.visited) > self.settings.max_scraped_count:
                    break

                enriched: List[List[str]] = list(map(lambda r: enrich_text(get_text(r)), results))
                await save_documents(enriched, results, Scraper.logger)
                await save_words(enriched, Scraper.logger)

                processed: Set[str] = self._filter_urls({result.url.path for result in results})
                await dump_visited_urls(processed, Scraper.logger)
                self.visited |= processed

                found_refs: Set[str] = self._filter_urls(self._find_hrefs(results))
                await dump_pending_urls(found_refs, Scraper.logger)
                self.queue.extend(found_refs)

                Scraper.logger.info("Scraped: %s", len(self.visited))

        await save_bigrams()
