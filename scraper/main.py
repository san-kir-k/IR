import asyncio
from typing import Dict

from scraper import Scraper
from utils import Settings, ArgParser


if __name__ == "__main__":
    args: Dict = ArgParser().parse()
    s: Scraper = Scraper(settings=Settings(**args))
    try:
        asyncio.run(s.run())
    except KeyboardInterrupt:
        pass
