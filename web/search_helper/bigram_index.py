from typing import List, Dict, Set
from logging import Logger

from db import get_words_by_bigrams, check_if_exists

from search_helper.common import get_bigrams
from search_helper.metrics import jaccard_coef, get_bound


class BigramIndex:
    def __init__(self, enriched_request: List[str], bound: int = 5) -> None:
        self.req = enriched_request
        self.bound = bound
        self.search_dict: Dict = dict()

    async def build(self, logger: Logger) -> None:
        for word in self.req:
            if await check_if_exists(word):
                logger.debug('Word "%s" exists', word)
                self.search_dict[word] = {word}
                continue

            self.search_dict[word] = set()
            bigrams: Set[str] = get_bigrams(word)
            index: Dict = await get_words_by_bigrams(bigrams)

            words: Set[str] = set()
            for _, others in index.items():
                for other in others:
                    words.add(other)

            coefs: List[List[float]] = []
            for other in words:
                coefs.append([jaccard_coef(word, other), other])
            coefs.sort(reverse=True)

            bound: float = get_bound(word)
            for coef, supposed in coefs[:self.bound]:
                logger.debug('Supposed word "%s" with coef %s', supposed, coef)
                if coef >= bound:
                    self.search_dict[word].add(supposed)
                else:
                    self.search_dict[word].add(word)

    def get_supposed(self, word: str) -> Set[str]:
        return self.search_dict[word]

    def get_search_dict(self) -> Dict:
        return self.search_dict
