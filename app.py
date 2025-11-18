from flask import Flask, render_template, request, url_for, redirect
from search import search_query

app = Flask(__name__)

@app.route('/')
def search():
    return render_template('index.html')


@app.route('/api/search')
def api_search():
    query = request.args.get('query')
    results = search_query(query) if query else []
    # sample result
    return {"results": results}


if __name__ == '__main__':
    app.run(debug=True)