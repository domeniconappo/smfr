import logging
from flask import render_template, redirect
from start import app

from smfrcore.client.api_client import ApiLocalClient, SMFRRestException
from smfrcore.client.conf import LOGGER_FORMAT, DATE_FORMAT
from forms import NewCollectorForm
from utils import MessageClass, add_message


logging.basicConfig(level=logging.INFO, format=LOGGER_FORMAT, datefmt=DATE_FORMAT)
logger = logging.getLogger(__name__)
client = ApiLocalClient()


@app.route('/', methods=('GET',))
def index():
    return render_template('index.html')


@app.route('/list', methods=('GET',))
def list_collections():
    res = client.list_collections()
    return render_template('list.html', collectors=res), 200


@app.route('/_running', methods=('GET',))
def list_active_collections():
    res = client.list_running_collectors()
    return render_template('list.html', collectors=res), 200


@app.route('/stopped', methods=('GET',))
def list_inactive_collections():
    res = client.list_inactive_collectors()
    return render_template('list.html', collectors=res), 200


@app.route('/new', methods=('GET', 'POST',))
def new_collection():
    form = NewCollectorForm()
    if form.validate_on_submit():
        payload = {
            'config': form.config.data, 'kwfile': form.kwfile.data, 'locfile': form.locfile.data,
            'runtime': form.runtime.data, 'trigger': form.trigger.data, 'nuts3': form.nuts3.data,
            'forecast': form.forecast_id.data, 'nuts3source': form.nuts3source.data, 'tzclient': form.tzclient.data,
        }
        try:
            res = client.new_collection(payload)
            logger.info(res)
            add_message('A new collection was added.', category=MessageClass.SUCCESS)
            return redirect('/list')
        except SMFRRestException as e:
            add_message('An error occurred: {}'.format(e), category=MessageClass.ERROR)
            return render_template('new_collection.html', form=form)
    return render_template('new_collection.html', form=form), 200


@app.route('/start/<int:collector_id>', methods=('GET',))
def start_collector(collector_id):
    try:
        res = client.start_collector(collector_id)
        add_message('The collection was started (collector id {})'.format(res), category=MessageClass.SUCCESS)
    except SMFRRestException as e:
        add_message('An error occurred: {}'.format(e), category=MessageClass.ERROR)
        logger.error(str(e))
    finally:
        return redirect('/list')


@app.route('/startall', methods=('GET',))
def start_all():
    client.start_all()
    return redirect('/list')


@app.route('/stopall', methods=('GET',))
def stop_all():
    client.stop_all()
    return redirect('/list')


@app.route('/stop/<int:collector_id>', methods=('GET',))
def stop_collector(collector_id):
    try:
        client.stop_collector(collector_id)
        add_message('The collection was stopped (collector id {})'.format(collector_id), category=MessageClass.SUCCESS)
    except SMFRRestException as e:
        add_message('An error occurred: {}'.format(e), category=MessageClass.ERROR)
        logger.error(str(e))
    finally:
        return redirect('/list')


@app.route('/remove/<int:collection_id>', methods=('GET',))
def remove_collection(collection_id):
    try:
        client.remove_collection(collection_id)
        add_message('The collection was removed from SMFR.', category=MessageClass.SUCCESS)
    except SMFRRestException as e:
        add_message('An error occurred: {}'.format(e), category=MessageClass.ERROR)
        logger.error(str(e))
    finally:
        return redirect('/list')


@app.route('/details/<int:collection_id>', methods=('GET',))
def collection_details(collection_id):
    try:
        res = client.get_collection(collection_id)
    except SMFRRestException as e:
        add_message('An error occurred: {}'.format(e), category=MessageClass.ERROR)
        logger.error(str(e))
        return redirect('/list')
    else:
        return render_template('details.html', data=res), 200


@app.route('/annotate/<int:collection_id>/<string:lang>', methods=('GET',))
def annotate_collection(collection_id, lang):
    try:
        client.start_annotation(collection_id, lang)
    except SMFRRestException as e:
        add_message('An error occurred: {}'.format(e), category=MessageClass.ERROR)
        logger.error(str(e))
    else:
        add_message('Classification started for collection', category=MessageClass.SUCCESS)
    finally:
        return redirect('/details/{}'.format(collection_id))


@app.route('/geolocalize/<int:collection_id>', methods=('GET',))
def geolocalize_collection(collection_id):
    try:
        client.start_geotagging(collection_id)
    except SMFRRestException as e:
        add_message('An error occurred: {}'.format(e), category=MessageClass.ERROR)
        logger.error(str(e))
    else:
        add_message('Geocoding started for collection', category=MessageClass.SUCCESS)
    finally:
        return redirect('/details/{}'.format(collection_id))


@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404
