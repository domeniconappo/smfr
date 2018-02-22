"""
Module to handle /collections API
"""
import copy
import os
import logging
import uuid
from functools import partial

import connexion
from flask import request

from daemons.collector import Collector
from errors import SMFRDBError
from server.config import LOGGER_FORMAT, DATE_FORMAT, CONFIG_STORE_PATH
from server.models import StoredCollector, VirtualTwitterCollection
from server.api import utils

from client.marshmallow import CollectorResponse, Collection

logging.basicConfig(level=logging.INFO, format=LOGGER_FORMAT, datefmt=DATE_FORMAT)
logger = logging.getLogger(__name__)


def add_collection(payload):
    """
    POST /collections
    Create a new Collection and start the relative Collector
    :param payload: a CollectorPayload object
    :return:
    """
    payload = connexion.request.form.to_dict()
    logger.info(payload)
    if not payload.get('forecast'):
        payload['forecast'] = 123456789

    CollectorClass = Collector.get_collector_class(payload['trigger'])

    logger.info('REQUESTS FILES')
    logger.info(connexion.request.files)
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

    logger.info('NORMALIZE PAYLOAD!')
    logger.info(payload)
    payload = utils.normalize_payload(payload)
    collector = CollectorClass.from_payload(payload)
    # # The collector/collection objects are created only, there are no processes starting at this moment
    # # (so we comment the call to Collector.launch() method)
    # # collector.launch()
    stored = StoredCollector(collection_id=collector.collection.id, parameters=payload)
    stored.save()
    collector.stored_instance = stored
    out_schema = CollectorResponse()
    res = out_schema.dump({'collection': collector.collection, 'id': stored.id}).data
    return res, 201
    # # TODO save files on REST Server and set the paths in payload like:
    # # payload['kwfile'] = local path to the copied file on server
    # payload = utils.normalize_payload(payload)
    # out_schema = CollectorResponse()
    #
    # CollectorClass = Collector.get_collector_class(payload['trigger'])
    # if not payload.get('forecast'):
    #     payload['forecast'] = 123456789
    # collector = CollectorClass.from_payload(payload)
    #
    # # The collector/collection objects are created only, there are no processes starting at this moment
    # # (so we comment the call to Collector.launch() method)
    # # collector.launch()
    #
    # stored = StoredCollector(collection_id=collector.collection.id, parameters=payload)
    # stored.save()
    # collector.stored_instance = stored
    # res = out_schema.dump({'collection': collector.collection, 'id': stored.id}).data
    # return res, 201


def get():
    """
    GET /collections
    Get all collections stored in DB (active and not active)
    :return:
    """
    coll_schema = Collection()
    res = VirtualTwitterCollection.query.all()
    res = coll_schema.dump(res, many=True).data
    return res, 200


def get_running_collectors():
    """
    GET /collections/active
    Get running collections/collectors
    :return:
    """
    out_schema = CollectorResponse()
    res = Collector.running_instances()
    res = [{'id': c.stored_instance.id, 'collection': c.collection} for c in res.values() if c]
    res = out_schema.dump(res, many=True).data
    return res, 200


def stop_collector(collector_id):
    """
    POST /collections/stop/{collector_id}
    Stop an existing and running collection
    :param collector_id:
    :return:
    """
    if not Collector.is_running(collector_id):
        return {}, 204

    res = Collector.running_instances()
    for collector in res.values():
        if collector_id == collector.stored_instance.id:
            collector.stop()
            return {}, 204

    return {'error': {'description': 'No collector with this id was found'}}, 404


def start_collector(collector_id):
    """
    POST /collections/start/{collector_id}
    Start an existing and stopped collection
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


def remove_collection(collector_id):
    """
    POST /collections/remove/{collector_id}
    Remove a collection from DB by using its collector_id
    :param collector_id: int
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


def start_all():
    """
    POST /collections/start
    Start all collections
    :return:
    """
    collectors = Collector.start_all()
    for c in collectors:
        c.launch()
    return {}, 204


def stop_all():
    """
    POST /collections/stop
    Stop all running collections
    :return:
    """

    res = Collector.running_instances()
    for collector in res.values():
        collector.stop()
    return {}, 204


def test_upload():
    logger.info('FILES %s', str(request.files))
    logger.info('DATA %s', str(request.data))
    logger.info('JSON %s', str(request.json))
    logger.info('HEADERS %s', str(request.headers))
    logger.info('FORM %s', str(request.form))
    file = request.files.get('kwfile')
    logger.info(file.read())
    return {}, 204
