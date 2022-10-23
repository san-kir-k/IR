import argparse
import logging
from typing import Dict

from utils.settings import Settings


class ArgParser:
    def __init__(self) -> None:
        self.parser = argparse.ArgumentParser()

        self.parser.add_argument(
            '-d', '--debug',
            help="Debug level of logging",
            action="store_const",
            dest="log_level",
            const=logging.DEBUG,
            default=Settings.log_level
        )

        self.parser.add_argument(
            '--rps',
            help='Requests per second',
            type=int,
            default=Settings.rps
        )

        self.parser.add_argument(
            '--batch_size',
            help='Batch size',
            type=int,
            default=Settings.batch_size
        )

        self.parser.add_argument(
            '--out',
            help='Output directory',
            type=str,
            dest="out_dir",
            default=Settings.out_dir
        )

        self.parser.add_argument(
            '--docs_count',
            help='Count of documents to be scraped',
            type=int,
            dest="max_scraped_count",
            default=Settings.max_scraped_count
        )

    def parse(self) -> Dict:
        return vars(self.parser.parse_args())
