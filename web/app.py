from flask import Flask, render_template, request, url_for, flash, redirect

from search_helper import BigramIndex
from request_enrich import get_enriched_words
from db import get_documents

from utils import Settings
from typing import List, Dict

import coloredlogs
import logging
import asyncio
import httpx


loop = asyncio.get_event_loop()

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)
coloredlogs.install(level=logging.DEBUG)

app = Flask(__name__)
app.config['SECRET_KEY'] = Settings().secret


@app.route('/', methods=('GET', 'POST'))
def search():
    if request.method == 'POST':
        search_request = request.form['request']
        enriched_request: List[str] = get_enriched_words(search_request)

        bi: BigramIndex = BigramIndex(enriched_request)
        loop.run_until_complete(bi.build(logger))

        search_dict: Dict = bi.get_search_dict()
        logger.debug("Supposed request structure: %s", search_dict)

        search_engine_request: List = [supposed[0] for supposed in search_dict.values()]
        logger.debug("Request for search engine: %s", search_engine_request)

        search_engine_response = httpx.post('http://localhost:8080/search', json={'words': search_engine_request})

        doc_ids: List = search_engine_response.json()["doc_ids"]
        logger.debug("Got %s documents in search engine", len(doc_ids))
        results = loop.run_until_complete(get_documents(doc_ids))
        logger.debug("Response: %s", results)

        if not search_request:
            flash('Request is required!')
        else:
            return render_template('result.html', results=results)

    return render_template('search.html')


if __name__ == '__main__':
    app.run(debug=False, use_reloader=False)
