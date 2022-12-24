import nltk
import re
import bs4

from httpx import Response
from bs4 import BeautifulSoup
from typing import Set, List

from nltk import tokenize, stem

from nltk.corpus import stopwords


def get_text(response: Response) -> str:
    if not response:
        return ""

    soup = BeautifulSoup(response.text, features="html.parser")
    output = ""
    for t in soup.find(id='mw-content-text').find_all(text=True):
        if isinstance(t, (bs4.Comment, bs4.Declaration, bs4.Stylesheet, bs4.Script)):
            continue
        try:
            if 'noprint' in t.parent['class']:
                continue
        except:
            pass
        if t.parent.name == 'a':
            continue
        output += str(t) + " "
    return re.sub("[^\w\s]", " ", output)


def enrich_text(text: str) -> List[str]:
    result: List[str] = []
    exclude_set: Set = {"DT", "EX", "UH", "MD", "IN"}
    stops: Set = set(stopwords.words("english"))

    stemmer = stem.PorterStemmer()

    tokenized = tokenize.sent_tokenize(text)
    for sen in tokenized:
        words_list: List[str] = tokenize.word_tokenize(sen)
        words_list = [p for p in nltk.pos_tag(words_list)]
        words_list = [w.lower() for w, ps in words_list
                      if w not in stops and w.isascii() and ps not in exclude_set]
        result += [stemmer.stem(w) for w in words_list]

    return result
