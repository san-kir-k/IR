from flask import Flask, render_template, request, url_for, flash, redirect

from search_helper import BigramIndex
from request_enrich import get_enriched_words

from utils import Settings
from typing import List

import coloredlogs
import logging
import asyncio


loop = asyncio.get_event_loop()

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)
coloredlogs.install(level=logging.DEBUG)

app = Flask(__name__)
app.config['SECRET_KEY'] = Settings().secret

results = [{'url': "https://ru.wikipedia.org/wiki/Диаграмма_Герцшпрунга_—_Рассела",
            'title': "Diagram"}]


@app.route('/', methods=('GET', 'POST'))
def search():
    if request.method == 'POST':
        search_request = request.form['request']
        enriched_request: List[str] = get_enriched_words(search_request)

        bi: BigramIndex = BigramIndex(enriched_request)
        loop.run_until_complete(bi.build(logger))

        logger.debug("Supposed request structure: %s", bi.get_search_dict())

        if not search_request:
            flash('Request is required!')
        else:
            results = [{'url': "https://ru.wikipedia.org/wiki/Заглавная_страница",
                        "title": "Main page"}]
            return render_template('result.html', results=results)

    return render_template('search.html')


if __name__ == '__main__':
    app.run(debug=False, use_reloader=False)
