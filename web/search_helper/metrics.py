from typing import Set

from search_helper.common import get_bigrams


def get_bound(word: str):
    if len(word) <= 5:
        return 0.25
    elif len(word) <= 8:
        return 0.4
    elif len(word) <= 12:
        return 0.545
    else:
        return 0.7


def jaccard_coef(lhs: str, rhs: str):
    lhs_bigrams: Set[str] = get_bigrams(lhs)
    rhs_bigrams: Set[str] = get_bigrams(rhs)

    coef: float = (len(lhs_bigrams & rhs_bigrams) / len(lhs_bigrams | rhs_bigrams) + 0.5 * len(set(lhs) & set(rhs)) / len(set(lhs) | set(rhs)) + 2 ** (min(len(lhs), len(rhs)) / max(len(lhs), len(rhs)) - 1) + sum(1 for a, b in zip(lhs, rhs) if a == b) / max(len(lhs), len(rhs))) / 4.0
    return coef
