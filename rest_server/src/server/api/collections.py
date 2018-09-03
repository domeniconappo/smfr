"""
Module to handle /collections API
"""
import os
import logging

import ujson as json
from flask import abort
from flask_jwt_extended import jwt_required, get_jwt_identity

from smfrcore.models import TwitterCollection, User, Nuts3, Nuts2, Aggregation, Tweet
from smfrcore.client.marshmallow import Collection as CollectionSchema, Aggregation as AggregationSchema
from smfrcore.client.ftp import FTPEfas
from sqlalchemy.exc import OperationalError

from daemons.collector import Collector

from smfrcore.errors import SMFRDBError, SMFRRestException

from server.api.clients import AnnotatorClient, GeocoderClient
from server.api.decorators import check_identity, check_role
from server.config import NUM_SAMPLES, RestServerConfiguration
from server.helpers import add_collection_helper, add_collection_from_rra_event

logger = logging.getLogger('RestServer API')


# @jwt_required
def add_collection(payload):
    """
    POST /collections
    Create a new Collection and start the associated Collector if runtime is specified.
    :param payload: a CollectorPayload schema object (plain text)
    :return: the created collection as a dict, 201
    """
    payload = json.loads(payload) if payload and not isinstance(payload, dict) else payload or {}
    if not payload.get('keywords'):
        payload['languages'], payload['keywords'] = RestServerConfiguration.default_keywords()
    payload['timezone'] = payload.get('tzclient') or '+00:00'
    collection = add_collection_helper(**payload)
    res = CollectionSchema().dump(collection).data
    return res, 201


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
    collections = []
    for event in payload:
        collection = add_collection_from_rra_event(**event)
        collections.append(collection)
    return CollectionSchema().dump(collections, many=True).data, 201


def get():
    """
    GET /collections
    Get all collections stored in DB (active and not active)
    :return:
    """
    try:
        collections = TwitterCollection.query.all()
        res = CollectionSchema().dump(collections, many=True).data
    except OperationalError:
        return {'error': {'description': 'DB link was lost. Try again'}}, 500
    else:
        return res, 200


def get_running_collections():
    """
    GET /collections/active
    Get running collections/collectors
    :return:
    """
    out_schema = CollectionSchema()
    res = Collector.running_instances()
    res = [c.collection for _, c in res]
    res = out_schema.dump(res, many=True).data
    return res, 200


# @check_identity
# @jwt_required
def stop_collection(collection_id):
    """
    POST /collections/{collection_id}/stop
    Stop an existing and running collection
    :param collection_id:
    :return:
    """
    if not Collector.is_running(collection_id):
        return {}, 204

    res = Collector.running_instances()
    for _, collector in res:
        if collection_id == collector.collection.id:
            collector.stop()
            return {}, 204

    # A collection is Active but its collector is not running. Update its status only.
    try:
        collector = Collector.resume(collection_id)
        collector.collection.deactivate()
        return {}, 204
    except SMFRDBError:
        return {'error': {'description': 'No collection with id {} was found for stopping'.format(collection_id)}}, 404


# @check_identity
# @jwt_required
def start_collection(collection_id):
    """
    POST /collections/start/{collection_id}
    Start an existing collection by resuming its associated collector
    :param collection_id:
    :return:
    """
    if Collector.is_running(collection_id):
        return {}, 204
    try:
        collector = Collector.resume(collection_id)
    except SMFRDBError:
        return {'error': {'description': 'No collection with id {} was found for starting'.format(collection_id)}}, 404

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
    try:
        collection = TwitterCollection.query.get(collection_id)
        if not collection:
            return {'error': {'description': 'No collector with this id was found'}}, 404
        aggregation = Aggregation.query.filter_by(collection_id=collection.id).first()

        collection_schema = CollectionSchema()
        collection_dump = collection_schema.dump(collection).data
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

        res = {'collection': collection_dump, 'datatable': samples_table,
               'aggregation': aggregation_dump, 'annotation_models': AnnotatorClient.models()[0]['models'],
               'running_annotators': AnnotatorClient.running()[0], 'running_geotaggers': GeocoderClient.running()[0],
               'datatableannotated': annotated_table, 'datatablegeotagged': geotagged_table}
    except OperationalError:
        return {'error': {'description': 'DB link was lost. Try again'}}, 500
    else:
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
def annotate(collection_id, startdate=None, enddate=None):
    """

    :param collection_id:
    :param startdate:
    :param enddate:
    :return:
    """
    try:
        res, code = AnnotatorClient.start(collection_id, start_date=startdate, end_date=enddate)
    except SMFRRestException as e:
        return {'error': {'description': str(e)}}, 500
    else:
        return res, code


# @check_identity
# @jwt_required
def stopgeolocalize(collection_id):
    """

    :param collection_id:
    :return:
    """
    try:
        res, code = GeocoderClient.stop(collection_id)
    except SMFRRestException as e:
        return {'error': {'description': str(e)}}, 500
    else:
        return res, code


# @check_identity
# @jwt_required
def stopannotate(collection_id):
    """

    :param collection_id:
    :return:
    """
    try:
        res, code = AnnotatorClient.stop(collection_id)
    except SMFRRestException as e:
        return {'error': {'description': str(e)}}, 500
    else:
        return res, code


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
                    Nuts3.country_name, Nuts3.nuts_id, Nuts3.name_ascii
                ).filter_by(efas_id=efas_id))
                if not nuts3_data:
                    logger.info('No NUTS3 data found for RRA event id %d', efas_id)
                    continue
                bbox = Nuts2.nuts2_bbox(efas_id)
                country_name = nuts3_data[0][0] if nuts3_data else ''
                nuts_id = nuts3_data[0][1] if nuts3_data else ''
                cities = set([c[2] for c in nuts3_data if c and c[2]])
                cities = ','.join(c for c in cities)
                if event['ID'] not in results:
                    results[event['ID']] = {'efas_id': efas_id, 'trigger': 'on-demand',
                                            'nuts': nuts_id, 'country': country_name, 'lead_time': lead_time,
                                            'keywords': cities, 'bbox': bbox, 'forecast': date}
                else:
                    results[event['ID']].update({'keywords': cities})
    return {'results': results}, 200
