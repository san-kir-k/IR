from flask import Flask, render_template, request, url_for, flash, redirect

from utils import Settings

app = Flask(__name__)
app.config['SECRET_KEY'] = Settings().secret

results = [{'reference': "https://ru.wikipedia.org/wiki/Диаграмма_Герцшпрунга_—_Рассела"}]


@app.route('/', methods=('GET', 'POST'))
def search():
    if request.method == 'POST':
        search_request = request.form['request']

        if not search_request:
            flash('Request is required!')
        else:
            results = [{'reference': "https://ru.wikipedia.org/wiki/Заглавная_страница"}]
            return render_template('result.html', results=results)

    return render_template('search.html')


if __name__ == '__main__':
    app.run()
