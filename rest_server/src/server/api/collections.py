"""
Module to handle /collections API
"""
import os
import logging
import uuid
import csv
from functools import partial

import connexion
from flask import abort
from flask_jwt_extended import jwt_required, get_jwt_identity

from smfrcore.models.sqlmodels import StoredCollector, TwitterCollection, User, Nuts3, NutsBoundingBox
from smfrcore.models.cassandramodels import Tweet
from smfrcore.client.marshmallow import Collector as CollectorSchema, CollectorResponse, Collection
from smfrcore.client.ftp import FTPEfas

from daemons.collector import Collector

from smfrcore.errors import SMFRDBError, SMFRRestException

from server.api.clients import AnnotatorClient, GeocoderClient
from server.api.decorators import check_identity, check_role
from server.config import CONFIG_STORE_PATH, CONFIG_FOLDER
from server.api import utils


logger = logging.getLogger(__name__)


# @jwt_required
def add_collection(payload):
    """
    POST /collections
    Create a new Collection and start the associated Collector if runtime is specified.
    :param payload: a CollectorPayload object
    :return: the created collection as a dict, 201
    """
    payload = connexion.request.form.to_dict()
    if not payload.get('forecast'):
        payload['forecast'] = 123456789

    CollectorClass = Collector.get_collector_class(payload['trigger'])

    iden = uuid.uuid4()
    tpl_filename = partial('{}_{}.yaml'.format, iden)

    # 1 copy files locally  on server and set paths
    # 2 set the payload to build Collector object
    for keyname, fs in connexion.request.files.items():
        if fs:
            filename = tpl_filename(fs.name)
            path = os.path.join(CONFIG_STORE_PATH, filename)
            fs.save(path)
            payload[keyname] = path

    payload = utils.normalize_payload(payload)
    collector = CollectorClass.from_payload(payload, user=None)

    # # The collector/collection objects are only instantiated at this point,
    # there are no processes starting here, unless it's a collector process with runtime
    # (i.e. start now and finish at the datetime defined in runtime)
    if collector.runtime:
        # launch the collector at creation time only when runtime parameter is set.
        # In all other cases, the collection process is being started manually from interface/cli
        collector.launch()

    stored = StoredCollector(collection_id=collector.collection.id, parameters=payload)
    stored.save()
    collector.stored_instance = stored
    out_schema = CollectorResponse()
    res = out_schema.dump({'collection': collector.collection, 'id': stored.id}).data
    return res, 201


def get():
    """
    GET /collections
    Get all collections stored in DB (active and not active)
    :return:
    """
    collectors = StoredCollector.query.join(TwitterCollection, StoredCollector.collection_id == TwitterCollection.id)
    coll_schema = CollectorResponse()
    res = [{'id': c.id, 'collection': c.collection} for c in collectors]
    res = coll_schema.dump(res, many=True).data
    return res, 200


def get_running_collectors():
    """
    GET /collections/active
    Get _running collections/collectors
    :return:
    """
    out_schema = CollectorResponse()
    res = Collector.running_instances()
    res = [{'id': c.stored_instance.id, 'collection': c.collection} for _, c in res]
    res = out_schema.dump(res, many=True).data
    return res, 200


def get_stopped_collectors():
    """
    GET /collections/inactive
    Get _running collections/collectors
    :return:
    """
    out_schema = CollectorResponse()
    collectors = Collector.resume_inactive()
    res = [{'id': c.stored_instance.id, 'collection': c.collection} for c in collectors]
    res = out_schema.dump(res, many=True).data
    return res, 200


# @check_identity
# @jwt_required
def stop_collector(collector_id):
    """
    POST /collections/stop/{collector_id}
    Stop an existing and _running collection
    :param collector_id:
    :return:
    """
    if not Collector.is_running(collector_id):
        return {}, 204

    res = Collector.running_instances()
    for _, collector in res:
        if collector_id == collector.stored_instance.id:
            collector.stop()
            return {}, 204

    # A collection is Active but its collector is not _running. Update its status only.
    try:
        collector = Collector.resume(collector_id)
        collector.collection.deactivate()
        return {}, 204
    except SMFRDBError:
        return {'error': {'description': 'No collector with this id was found'}}, 404


# @check_identity
# @jwt_required
def start_collector(collector_id):
    """
    POST /collections/{collector_id}/start
    Start an existing collection by resuming its assoicated collector
    :param collector_id:
    :return:
    """
    if Collector.is_running(collector_id):
        return {}, 204
    try:
        collector = Collector.resume(collector_id)
    except SMFRDBError:
        return {'error': {'description': 'No collector with this id was found'}}, 404

    collector.launch()

    return {}, 204


# @check_identity
# @jwt_required
def remove_collection(collection_id):
    """
    POST /collections/{collection_id}/remove
    Remove a collection from DB
    :param collection_id: int
    :return:
    """
    collection = TwitterCollection.query.get(collection_id)
    if not collection:
        return {'error': {'description': 'No collector with this id was found'}}, 404
    stored = StoredCollector.query.filter_by(collection_id=collection.id).first()
    if stored:
        collector_params = stored.parameters
        for k in ('kwfile', 'locfile', 'config'):
            path = collector_params.get(k)
            if path and os.path.exists(path):
                os.unlink(path)
        stored.delete()
    collection.delete()
    return {}, 204


# @check_identity
# @jwt_required
def get_collection_details(collection_id):
    """
    GET /collections/{collection_id}/details
    :param collection_id: int
    :return: A CollectionResponse marshmallow object
    """
    num_samples = 100
    collection = TwitterCollection.query.get(collection_id)
    if not collection:
        return {'error': {'description': 'No collector with this id was found'}}, 404
    collector = StoredCollector.query.filter_by(collection_id=collection.id).first()

    collection_schema = Collection()
    collection_dump = collection_schema.dump(collection).data
    collector_schema = CollectorSchema()
    collector_dump = collector_schema.dump(collector).data

    tweets = Tweet.get_samples(collection_id=collection_id, ttype='collected', size=num_samples)
    annotated_tweets = Tweet.get_samples(collection_id=collection_id, ttype='annotated', size=num_samples)
    geotagged_tweets = Tweet.get_samples(collection_id=collection_id, ttype='geotagged', size=num_samples)

    samples_table = []
    annotated_table = []
    geotagged_table = []

    for i, t in enumerate(tweets, start=1):
        samples_table.append(Tweet.make_table_object(i, t))

    for i, t in enumerate(annotated_tweets, start=1):
        annotated_table.append(Tweet.make_table_object(i, t))

    for i, t in enumerate(geotagged_tweets, start=1):
        geotagged_table.append(Tweet.make_table_object(i, t))

    res = {'collection': collection_dump, 'collector': collector_dump, 'datatable': samples_table,
           'running_annotators': AnnotatorClient.running()[0], 'running_geotaggers': GeocoderClient.running()[0],
           'datatableannotated': annotated_table, 'datatablegeotagged': geotagged_table}
    return res, 200


# @check_identity
# @jwt_required
def geolocalize(collection_id, startdate=None, enddate=None):
    """

    :param collection_id:
    :param startdate:
    :param enddate:
    :return:
    """
    try:
        res, code = GeocoderClient.start(collection_id)
    except SMFRRestException as e:
        return {'error': {'description': str(e)}}, 500
    else:
        return res, code


# @check_identity
# @jwt_required
def annotate(collection_id=None, lang='en', forecast_id=None, startdate=None, enddate=None):
    """

    :param collection_id:
    :param lang:
    :param forecast_id:
    :param startdate:
    :param enddate:
    :return:
    """
    try:
        res, code = AnnotatorClient.start(collection_id, lang)
    except SMFRRestException as e:
        return {'error': {'description': str(e)}}, 500
    else:
        return res, code


# @check_role
# @jwt_required
def start_all():
    """
    POST /collections/startall
    Start all inactive collections
    :return:
    """
    not_running_collectors = (c for c in Collector.resume_all() if not Collector.is_running(c.stored_instance.id))
    for c in not_running_collectors:
        c.launch()
    return {}, 204


# @check_role
# @jwt_required
def stop_all():
    """
    POST /collections/stopall
    Stop all _running collections
    :return:
    """

    res = Collector.running_instances()
    for _, collector in res:
        collector.stop()
    return {}, 204


# @check_role
# @jwt_required
def fetch_efas(since='latest'):
    """

    :param since:
    :return:
    """
    ftp_client = FTPEfas(since)
    ftp_client.download_rra()
    ftp_client.close()
    results = {}
    if os.path.getsize(ftp_client.localfilepath) > 0:
        # TODO create ondemand collections from EFAS events
        with open(ftp_client.localfilepath) as f:
            reader = csv.DictReader(f, delimiter=',')
            next(reader)  # skip header

            for event in reader:
                date = ftp_client.filename_date
                event_id = event['ID'].rstrip('.0')
                efas_id = int(float(event['ID']))
                bbox = NutsBoundingBox.nuts2_bbox(efas_id)
                cities = event.get('Cities') or event.get('1Cities') or ''
                # from EFAS RRA, cities come as '[Bassens/0.28%] [Bordeaux/1.7%]' strings
                cities = ','.join(c.replace('[', '').replace(']', '').split('/')[0] for c in cities.split())
                if not cities:
                    cities = list(Nuts3.query.with_entities(Nuts3.name_ascii).filter_by(efas_id=efas_id))
                    cities = ','.join(c[0] for c in cities if c and c[0])
                if not cities:
                    continue
                if event_id not in results:
                    results[event_id] = {'efas_id': efas_id, 'trigger': 'on-demand',
                                         'keywords': cities, 'bbox': bbox, 'forecast': date}
                else:
                    results[event_id].update({'keywords': cities})
    return {'results': results}, 200


# @check_role
# @jwt_required
def add_ondemand(payload):
    """

    :param payload: list of dict with following format
    {'bbox': {'max_lat': 40.6587, 'max_lon': -1.14236, 'min_lat': 39.2267, 'min_lon': -3.16142},
     'trigger': 'on-demand', 'forecast': '2018061500', 'keywords': 'Cuenca', 'efas_id': 1436
     }
    :type: list
    :return:
    """
    from daemons.collector import OndemandCollector
    collectors = []
    for event in payload:
        collector = OndemandCollector.create_from_event(event)
        collectors.append(str(collector))
        # params for stored collector is in the form of a dict:
        # {"trigger": "manual", "tzclient": "+02:00", "forecast": 123456789,
        #     "kwfile": "/path/c78e0f08-98c9-4553-b8ee-a41f15a34110_kwfile.yaml",
        #     "locfile": "/path/c78e0f08-98c9-4553-b8ee-a41f15a34110_locfile.yaml",
        #     "config": "/path/c78e0f08-98c9-4553-b8ee-a41f15a34110_config.yaml"}
        params = {'trigger': 'on-demand', 'forecast': event['forecast'],
                  'tzclient': '+00:00',
                  'kwfile': collector.kwfile, 'locfile': collector.locfile,
                  'config': collector.config}
        stored = StoredCollector(collection_id=collector.collection.id, parameters=params)
        stored.save()
        collector.stored_instance = stored
    return {'results': collectors}, 201
