"""
Module to handle /collections API
"""
import os
import logging

import ujson as json
from flask import abort
from flask_jwt_extended import jwt_required, get_jwt_identity

from smfrcore.models.sqlmodels import StoredCollector, TwitterCollection, User, Nuts3, Nuts2, Aggregation
from smfrcore.models.cassandramodels import Tweet
from smfrcore.client.marshmallow import Collector as CollectorSchema, CollectorResponse, Collection, Aggregation as AggregationSchema
from smfrcore.client.ftp import FTPEfas

from daemons.collector import Collector

from smfrcore.errors import SMFRDBError, SMFRRestException

from server.api.clients import AnnotatorClient, GeocoderClient
from server.api.decorators import check_identity, check_role
from server.config import NUM_SAMPLES


logger = logging.getLogger('RestServer API')


# @jwt_required
def add_collection(payload):
    """
    POST /collections
    Create a new Collection and start the associated Collector if runtime is specified.
    :param payload: a CollectorPayload object
    :return: the created collection as a dict, 201
    """
    payload = json.loads(payload) if payload else {}
    # if payload.get('forecast') is None:
    #     payload['forecast'] = ''
    CollectorClass = Collector.get_collector_class(payload['trigger'])
    collector = CollectorClass.from_payload(payload, user=None)
    if collector.runtime:
        # launch the collector at creation time only when runtime parameter is set.
        # In all other cases, the collection process is being started manually from interface/cli
        collector.launch()
    res = {'collection': CollectorResponse().dump(collector).data, 'id': collector.stored_instance.id}
    return res, 200


def get():
    """
    GET /collections
    Get all collections stored in DB (active and not active)
    :return:
    """
    collectors = StoredCollector.query.join(TwitterCollection, StoredCollector.collection_id == TwitterCollection.id)
    out_schema = CollectorResponse()
    res = out_schema.dump(collectors, many=True).data
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
        stored.delete()
    aggregation = Aggregation.query.filter_by(collection_id=collection.id).first()
    if aggregation:
        aggregation.delete()
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
    collection = TwitterCollection.query.get(collection_id)
    if not collection:
        return {'error': {'description': 'No collector with this id was found'}}, 404
    collector = StoredCollector.query.filter_by(collection_id=collection.id).first()
    aggregation = Aggregation.query.filter_by(collection_id=collection.id).first()

    collection_schema = Collection()
    collection_dump = collection_schema.dump(collection).data
    collector_schema = CollectorSchema()
    collector_dump = collector_schema.dump(collector).data
    aggregation_schema = AggregationSchema()
    aggregation_dump = aggregation_schema.dump(aggregation).data

    tweets = Tweet.get_samples(collection_id=collection_id, ttype='collected', size=NUM_SAMPLES)
    annotated_tweets = Tweet.get_samples(collection_id=collection_id, ttype='annotated', size=NUM_SAMPLES)
    geotagged_tweets = Tweet.get_samples(collection_id=collection_id, ttype='geotagged', size=NUM_SAMPLES)

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
           'aggregation': aggregation_dump, 'annotation_models': AnnotatorClient.models()[0]['models'],
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
        res, code = GeocoderClient.start(collection_id, startdate, enddate)
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
    # RRA file content
    # [{"ID":1414,"SM_meanLT":2.0},{"ID":1436,"SM_meanLT":3.0},{"ID":1673,"SM_meanLT":7.0}]
    if os.path.getsize(ftp_client.localfilepath) > 0:
        with open(ftp_client.localfilepath) as f:
            events = json.load(f)

            for event in events:
                date = ftp_client.filename_date
                efas_id = int(event['ID'])
                lead_time = int(event['SM_meanLT'])
                logger.info('Fetched event for efas id %d', efas_id)
                nuts3_data = list(Nuts3.query.with_entities(
                    Nuts3.names, Nuts3.country_name, Nuts3.nuts_id, Nuts3.name_ascii
                ).filter_by(efas_id=efas_id))
                if not nuts3_data:
                    logger.info('No NUTS3 data found for RRA event id %d', efas_id)
                    continue
                bbox = Nuts2.nuts2_bbox(efas_id)
                country_name = nuts3_data[0][1] if nuts3_data else ''
                nuts_id = nuts3_data[0][2] if nuts3_data else ''
                cities = set([s for c in nuts3_data for s in c[0].values() if c and c[0] and s] + [c[3] for c in nuts3_data if c and c[3]])
                cities = ','.join(c for c in cities)
                if event['ID'] not in results:
                    results[event['ID']] = {'efas_id': efas_id, 'trigger': 'on-demand',
                                            'nuts': nuts_id, 'country': country_name, 'lead_time': lead_time,
                                            'keywords': cities, 'bbox': bbox, 'forecast': date}
                else:
                    results[event['ID']].update({'keywords': cities})
    return {'results': results}, 200


# @check_role
# @jwt_required
def add_ondemand(payload):
    """
    Add a list of on demand collections running immediately which stop at given runtime parameter
    :param payload: list of dict with following format
    {'bbox': {'max_lat': 40.6587, 'max_lon': -1.14236, 'min_lat': 39.2267, 'min_lon': -3.16142},
     'trigger': 'on-demand', 'forecast': '2018061500', 'keywords': 'Cuenca', 'efas_id': 1436
     }
    :type: list
    :return:
    """
    from daemons.collector import OndemandCollector
    collections = []
    for event in payload:
        event['tz'] = '+00:00'
        collector = OndemandCollector.create_from_event(event)
        # params for stored collector is in the form of a dict:
        # {"trigger": "manual", "tzclient": "+02:00", "forecast": 123456789,
        #     "kwfile": "/path/c78e0f08-98c9-4553-b8ee-a41f15a34110_kwfile.yaml",
        #     "locfile": "/path/c78e0f08-98c9-4553-b8ee-a41f15a34110_locfile.yaml",
        #     "config": "/path/c78e0f08-98c9-4553-b8ee-a41f15a34110_config.yaml"}
        params = {'trigger': 'on-demand', 'forecast': event['forecast'],
                  'tzclient': '+00:00', 'runtime': collector.runtime,
                  'kwfile': collector.kwfile, 'locfile': collector.locfile,
                  'config': collector.config}
        collections.append({'efas id': event['efas_id'], 'runtime': collector.runtime, 'nuts': event.get('nuts'),
                            'keywords': event['keywords'], 'bbox': event['bbox']})
        stored = StoredCollector(collection_id=collector.collection.id, parameters=params)
        stored.save()
        collector.stored_instance = stored
        collector.launch()
    return {'results': collections}, 201
