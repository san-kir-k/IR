import asyncio
from aiolimiter import AsyncLimiter
from time import time
from httpx import AsyncClient
from bs4 import BeautifulSoup
from collections import deque

from typing import List, Set, Tuple


def find_hrefs(sources: Tuple) -> Set[str]:
    result: Set[str] = set()
    for s in sources:
        soup = BeautifulSoup(s.text, features="html.parser")
        result |= {a['href'] for a in soup.find_all('a', href=True)}
    return result


async def scrape(url, session, throttler):
    async with throttler:
        return await session.get(url)


URL = 'https://en.wikipedia.org'


def filter_urls(urls: Set[str], visited: Set[str]) -> Set[str]:
    return {f'{URL}{url}' for url in urls if url.startswith('/wiki/') and url not in visited}


async def run():
    _start = time()
    throttler = AsyncLimiter(max_rate=50, time_period=1)   # 1 tasks/second
    queue = deque(["https://en.wikipedia.org/wiki/Main_Page"])
    visited = {"https://en.wikipedia.org/wiki/Main_Page"}
    scraped_count = 0
    batch_size = 5
    async with AsyncClient() as session:
        while scraped_count < 20:
            tasks = []
            processed_in_batch = 0
            bound = min(len(queue), batch_size)
            while processed_in_batch < bound:
                url = queue.popleft()
                tasks.append(scrape(url, session=session, throttler=throttler))
                processed_in_batch += 1
            results = await asyncio.gather(*tasks)
            found_refs = filter_urls(find_hrefs(results), visited)
            queue.extend(found_refs)
            visited |= found_refs

            for i, html in enumerate(results):
                if not html.text:
                    continue

                with open(f'./out/{i + scraped_count}.txt', 'w') as f:
                    f.write(html.text)

            scraped_count += len(results)
            print(f"SCOUNT = {scraped_count}")


if __name__ == "__main__":
    asyncio.run(run())
