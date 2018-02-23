"""
Module to handle /collections API
"""
import os
import logging
import uuid
from functools import partial

import connexion

from daemons.collector import Collector
from errors import SMFRDBError
from server.config import LOGGER_FORMAT, DATE_FORMAT, CONFIG_STORE_PATH
from server.models import StoredCollector, VirtualTwitterCollection
from server.api import utils

from client.marshmallow import CollectorResponse

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


def get():
    """
    GET /collections
    Get all collections stored in DB (active and not active)
    :return:
    """
    stored_collectors = StoredCollector.query.join(VirtualTwitterCollection,
                                                   StoredCollector.collection_id == VirtualTwitterCollection.id)
    coll_schema = CollectorResponse()
    res = [{'id': c.id, 'collection': c.collection} for c in stored_collectors]
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


def get_stopped_collectors():
    """
    GET /collections/inactive
    Get running collections/collectors
    :return:
    """
    out_schema = CollectorResponse()
    collectors = Collector.resume_inactive()
    res = [{'id': c.stored_instance.id, 'collection': c.collection} for c in collectors]
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
        if collector and collector_id == collector.stored_instance.id:
            collector.stop()
            return {}, 204

    # A collection is Active but its collector is not running. Update its status only.
    try:
        collector = Collector.resume(collector_id)
        collector.collection.deactivate()
        return {}, 204
    except SMFRDBError:
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


def remove_collection(collection_id):
    """
    POST /collections/remove/{collection_id}
    Remove a collection from DB
    :param collection_id: int
    :return:
    """
    collection = VirtualTwitterCollection.query.get(collection_id)
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


def stop_all():
    """
    POST /collections/stopall
    Stop all running collections
    :return:
    """

    res = Collector.running_instances()
    for collector in res.values():
        collector.stop()
    return {}, 204
