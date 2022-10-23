import dataclasses
import logging


@dataclasses.dataclass
class Settings:
    log_level: int = logging.INFO
    start_url: str = "https://en.wikipedia.org/wiki/Main_Page"
    url_base: str = "https://en.wikipedia.org"
    rps: int = 20
    batch_size: int = 5
    out_dir: str = 'out'
    max_scraped_count: int = 20
