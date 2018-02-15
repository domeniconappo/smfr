from flask import render_template
from start import app

from client.api_client import ApiLocalClient


@app.route('/')
@app.route('/list')
def index():
    client = ApiLocalClient()
    res = client.list_collections()
    return render_template('index.html', collections=res)


@app.route('/running')
def running():
    client = ApiLocalClient()
    res = client.list_running_collectors()
    return render_template('index.html', collections=res)


@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404
