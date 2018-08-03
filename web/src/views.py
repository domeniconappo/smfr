import logging

import ujson as json

from flask import render_template, redirect, request

from smfrcore.client.api_client import ApiLocalClient, SMFRRestException
from smfrcore.utils import LOGGER_FORMAT, LOGGER_DATE_FORMAT

from forms import NewCollectorForm, ExportForm
from utils import MessageClass, add_message
from start import app


logging.basicConfig(level=logging.INFO, format=LOGGER_FORMAT, datefmt=LOGGER_DATE_FORMAT)
logger = logging.getLogger('Web')
client = ApiLocalClient()


@app.route('/', methods=('GET',))
def index():
    return render_template('index.html'), 200


@app.route('/admin', methods=('GET',))
def admin():
    return render_template('admin.html'), 200


@app.route('/fetch_efas', methods=('GET', 'POST'))
def fetch_efas():
    res = {}
    status_code = 500
    if request.method == 'POST':
        try:
            tzclient = request.form.get('tzclient', '+00:00')
            selected_events = request.form.getlist('events')
            selected_events = [json.loads(e.replace('\'', '"')) for e in selected_events]
            [e.update({'tzclient': tzclient}) for e in selected_events]
            res, status_code = client.add_ondemand_collections(selected_events)
            add_message(res, category=MessageClass.SUCCESS)
            return redirect('/list')
        except SMFRRestException as e:
            add_message('An error occurred: {}'.format(e), category=MessageClass.ERROR)
            return render_template('admin.html', fetched_events=res.get('results')), status_code
    try:
        since = request.args.get('since') or 'latest'
        res, status_code = client.fetch_efas(since)
    except SMFRRestException as e:
        add_message('An error occurred: {}'.format(e), category=MessageClass.ERROR)
    finally:
        return render_template('admin.html', fetched_events=res.get('results')), status_code


@app.route('/list', methods=('GET',))
def list_collections():
    res = client.list_collections()
    return render_template('list.html', collections=res), 200


@app.route('/running', methods=('GET',))
def list_active_collections():
    res = client.list_running_collectors()
    return render_template('list.html', collectors=res), 200


@app.route('/stopped', methods=('GET',))
def list_inactive_collections():
    res = client.list_inactive_collectors()
    return render_template('list.html', collectors=res), 200


@app.route('/export/<int:collection_id>', methods=('GET', 'POST',))
def export_tweets(collection_id):
    # TODO
    form = ExportForm(collection_id=collection_id)
    if form.validate_on_submit():
        pass
    return render_template('export.html', form=form), 200


@app.route('/new', methods=('GET', 'POST',))
def new_collection():
    form = NewCollectorForm()
    if form.validate_on_submit():
        payload = {
            'configuration': form.configuration.data, 'keywords': form.keywords.data,
            'bounding_box': form.bounding_box.data,
            'runtime': form.runtime.data, 'trigger': form.trigger.data, 'nuts2': form.nuts2.data,
            'forecast': form.forecast_id.data, 'tzclient': form.tzclient.data,
        }
        try:
            _ = client.new_collection(payload)
            add_message('A new collection was added.', category=MessageClass.SUCCESS)
        except SMFRRestException as e:
            add_message('An error occurred: {}'.format(e), category=MessageClass.ERROR)
            return render_template('new_collection.html', form=form)
        else:
            return redirect('/list')
    return render_template('new_collection.html', form=form), 200


@app.route('/start/<collection_id>', methods=('GET',))
def start_collector(collection_id):
    try:
        _ = client.start_collector(collection_id)
        add_message('The collection was started (collection id {})'.format(collection_id),
                    category=MessageClass.SUCCESS)
    except SMFRRestException as e:
        add_message('An error occurred: {}'.format(e), category=MessageClass.ERROR)
        logger.error(str(e))
    finally:
        return redirect('/details/{}'.format(collection_id))


@app.route('/stop/<int:collection_id>', methods=('GET',))
def stop_collector(collection_id):
    try:
        client.stop_collector(collection_id)
        add_message('The collection was stopped (collection id {})'.format(collection_id), category=MessageClass.SUCCESS)
    except SMFRRestException as e:
        add_message('An error occurred: {}'.format(e), category=MessageClass.ERROR)
        logger.error(str(e))
    finally:
        return redirect('/details/{}'.format(collection_id))


@app.route('/remove/<int:collection_id>', methods=('GET',))
def remove_collection(collection_id):
    logger.info('removing collection %d', collection_id)
    try:
        res, _ = client.remove_collection(collection_id)
        logger.info('removed collection %d %s', collection_id, res)
        add_message('The collection {} was removed from SMFR.'.format(collection_id), category=MessageClass.SUCCESS)
    except SMFRRestException as e:
        add_message('An error occurred: {}'.format(e), category=MessageClass.ERROR)
        logger.error(str(e))
    except Exception as e:
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
        add_message('Classification started for collection: language: {}'.format(lang), category=MessageClass.SUCCESS)
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
