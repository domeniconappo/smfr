import logging
from flask import render_template, request, redirect
from start import app

from client.api_client import ApiLocalClient, SMFRRestException
from client.conf import LOGGER_FORMAT, DATE_FORMAT
from forms import NewCollectorForm
from utils import MessageClass, add_message


logging.basicConfig(level=logging.INFO, format=LOGGER_FORMAT, datefmt=DATE_FORMAT)
logger = logging.getLogger(__name__)


@app.route('/', methods=('GET',))
@app.route('/list', methods=('GET',))
def list_collections():
    client = ApiLocalClient()
    res = client.list_collections()
    return render_template('list.html', collections=res)


@app.route('/running', methods=('GET',))
def running():
    client = ApiLocalClient()
    res = client.list_running_collectors()
    return render_template('list.html', collections=res)


@app.route('/new', methods=('GET', 'POST'))
def new_collection():
    form = NewCollectorForm()
    if form.validate_on_submit():
        client = ApiLocalClient()
        payload = {'config': form.config.data, 'kwfile': form.kwfile.data, 'locfile': form.locfile.data,
                   'runtime': form.runtime.data, 'trigger': form.trigger.data, 'nuts3': form.nuts3.data,
                   'forecast': form.forecast_id.data, 'nuts3source': form.nuts3source.data,
                   }
        try:
            res = client.new_collection(payload)
            logger.info(res)
            add_message('A new collection was added.', category=MessageClass.SUCCESS)
            return redirect('/list')
        except SMFRRestException as e:
            add_message('An error occurred: {}'.format(e), category=MessageClass.ERROR)
            return render_template('new_collection.html', form=form)
    return render_template('new_collection.html', form=form)


@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404
