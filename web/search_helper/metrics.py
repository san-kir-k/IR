from typing import Set, Dict

from search_helper.common import get_bigrams


def damerau_levenshtein_distance(lhs: str, rhs: str) -> float:
    d: Dict = dict()
    for i in range(-1, len(lhs) + 1):
        d[(i, -1)] = i + 1
    for j in range(-1, len(rhs) + 1):
        d[(-1, j)] = j + 1

    for i in range(len(lhs)):
        for j in range(len(rhs)):
            if lhs[i] == rhs[j]:
                cost = 0
            else:
                cost = 0.9
            d[(i, j)] = min(
                d[(i - 1, j)] + 1,
                d[(i, j - 1)] + 1,
                d[(i - 1, j - 1)] + cost,
            )
            if i and j and lhs[i] == rhs[j - 1] and lhs[i - 1] == rhs[j]:
                d[(i, j)] = min(d[(i, j)], d[i - 2, j - 2] + 1)

    return d[len(lhs) - 1, len(rhs) - 1]


def jaccard_coef(lhs: str, rhs: str):
    lhs_bigrams: Set[str] = get_bigrams(lhs)
    rhs_bigrams: Set[str] = get_bigrams(rhs)

    coef: float = (0.5 * len(lhs_bigrams & rhs_bigrams) / len(lhs_bigrams | rhs_bigrams)
                   + 0.5 * sum(1 for a, b in zip(lhs, rhs) if a == b) / max(len(lhs), len(rhs)))
    return coef
