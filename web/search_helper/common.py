from itertools import tee
from typing import Set


def get_bigrams(word: str) -> Set[str]:
    lhs, rhs = tee(word)
    next(rhs, None)
    return set([''.join(p) for p in list(zip(lhs, rhs))])
