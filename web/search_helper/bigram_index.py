from typing import List, Dict, Set
from logging import Logger

from db import get_words_by_bigrams, check_if_exists

from search_helper.common import get_bigrams
from search_helper.metrics import jaccard_coef, damerau_levenshtein_distance


class BigramIndex:
    def __init__(self, enriched_request: List[str],
                 count_bound: int = 3, distance_bound: float = 3) -> None:
        self.req = enriched_request
        self.count_bound = count_bound
        self.distance_bound = distance_bound
        self.search_dict: Dict = dict()

    async def build(self, logger: Logger) -> None:
        for word in self.req:
            if await check_if_exists(word):
                logger.debug('Word "%s" exists', word)
                self.search_dict[word] = [word]
                continue

            self.search_dict[word] = []
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

            most_similar: List = [w for _, w in coefs[:100]]
            distances: List[List[float]] = []
            for other in most_similar:
                distances.append([damerau_levenshtein_distance(word, other), other])
            distances.sort()

            for d, supposed in distances[:self.count_bound]:
                logger.debug('Supposed word "%s" with LD-distance %s', supposed, d)
                if d <= self.distance_bound:
                    self.search_dict[word].append(supposed)
                else:
                    self.search_dict[word].append(word)

    def get_supposed(self, word: str) -> Set[str]:
        return self.search_dict[word]

    def get_search_dict(self) -> Dict:
        return self.search_dict
