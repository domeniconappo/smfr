"""
Module to handle /collections API
"""

from daemons.collector import Collector
from errors import SMFRDBError
from server.models import StoredCollector, VirtualTwitterCollection
from server.api import utils

from client.marshmallow import CollectorResponse, Collection


def post(payload):
    """
    POST /collections
    Create a new Collection and start the relative Collector
    :param payload: a CollectorPayload object
    :return:
    """
    payload = utils.normalize_payload(payload)
    out_schema = CollectorResponse()
    CollectorClass = Collector.get_collector_class(payload['trigger'])
    if not payload.get('forecast'):
        payload['forecast'] = 123456789
    collector = CollectorClass.from_payload(payload)
    collector.launch()
    stored = StoredCollector(collection_id=collector.collection.id, parameters=payload)
    stored.save()
    collector.stored_instance = stored
    res = out_schema.dump({'collection': collector.collection, 'id': stored.id}).data
    return res, 201


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
