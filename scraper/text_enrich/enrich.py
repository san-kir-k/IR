import nltk
import re

from httpx import Response
from bs4 import BeautifulSoup
from typing import Set, List

from nltk import tokenize, stem

from nltk.corpus import stopwords


def get_text(response: Response) -> str:
    soup = BeautifulSoup(response.text, features="html.parser")
    return re.sub("[^\w\s]", " ", soup.get_text())


def enrich_text(text: str) -> List[str]:
    result: List[str] = []
    exclude_set: Set = {"DT", "EX", "UH", "MD"}
    stops: Set = set(stopwords.words("english"))

    stemmer = stem.PorterStemmer()

    tokenized = tokenize.sent_tokenize(text)
    for sen in tokenized:
        words_list: List[str] = tokenize.word_tokenize(sen)
        words_list = [p for p in nltk.pos_tag(words_list)]
        words_list = [w.lower() for w, _ in words_list if w not in stops]
        result += [stemmer.stem(w) for w in words_list]

    return result
