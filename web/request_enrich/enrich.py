import nltk
import re

from typing import Set, List

from nltk import tokenize, stem

from nltk.corpus import stopwords


def get_enriched_words(request: str) -> List[str]:
    request = re.sub("[^\w\s]", " ", request)

    result: List[str] = []
    exclude_set: Set = {"DT", "EX", "UH", "MD", "IN"}
    stops: Set = set(stopwords.words("english"))

    stemmer = stem.PorterStemmer()

    tokenized = tokenize.sent_tokenize(request)
    for sen in tokenized:
        words_list: List[str] = tokenize.word_tokenize(sen)
        words_list = [p for p in nltk.pos_tag(words_list)]
        words_list = [w.lower() for w, ps in words_list
                      if w not in stops and w.isascii() and ps not in exclude_set]
        result += [stemmer.stem(w) for w in words_list]

    return result
