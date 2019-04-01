import signal
import sys
import os

from flask_restful import Resource, Api, marshal_with, fields, marshal_with_field

from smfrcore.models.sql import create_app
from smfrcore.utils import FALSE_VALUES

from collector import BackgroundCollector, OnDemandCollector, ManualCollector, logger
from config import configuration_object
from jobs import schedule_rra_jobs, update_ondemand_collections_status

app = create_app(__name__)
api = Api(app)

app.app_context().push()


class CollectorsApi(Resource):
    """
    API for `/collectors` endpoint
    """

    @marshal_with_field(fields.Raw)
    def get(self, trigger_type):
        """
        GET /collectors
        Get all collectors
        :return:
        """

        def build_collector_item(c):
            return {
                'trigger_type': c.type,
                'apikeys': c.streamer.keys,
                'errors': c.streamer.errors,
                'status': 'connected' if c.streamer.is_connected.value == 1 else 'disconnected',
                'collections': [co.id for co in c.streamer.collections],
            }

        if trigger_type != 'all':
            collector = configuration_object.collectors.get(trigger_type, None)
            res = {'collectors': [build_collector_item(collector)]}
        else:
            collectors = []
            for tt, collector in configuration_object.collectors.items():
                collectors.append(build_collector_item(collector))
            res = {'collectors': collectors}
        return res, 200

    @marshal_with({'result': fields.Raw})
    def put(self, trigger_type):
        collector_to_restart = configuration_object.collectors[trigger_type]
        collector_to_restart.restart()
        return {'succes': True}, 201


def main():
    background_collector = BackgroundCollector()
    ondemand_collector = OnDemandCollector()
    manual_collector = ManualCollector()
    logger.debug('---------- Registering collectors in main configuration:\n%s',
                 [background_collector, ondemand_collector, manual_collector])
    configuration_object.set_collectors({background_collector.type: background_collector,
                                         ondemand_collector.type: ondemand_collector,
                                         manual_collector.type: manual_collector})

    update_ondemand_collections_status(restart_ondemand=False)

    start_collectors = os.getenv('START_COLLECTORS', 'no')
    if start_collectors not in FALSE_VALUES:
        background_collector.start()
        ondemand_collector.start()
        manual_collector.start()

        schedule_rra_jobs()

        def stop_active_collectors(signum, _):
            deactivate_collections = False
            logger.info("Received %d", signum)
            logger.info("Stopping any running collector...")
            background_collector.stop(deactivate=deactivate_collections)
            ondemand_collector.stop(deactivate=deactivate_collections)
            manual_collector.stop(deactivate=deactivate_collections)
            sys.exit(0)

        signals = (signal.SIGINT, signal.SIGTERM, signal.SIGQUIT)
        for sig in signals:
            signal.signal(sig, stop_active_collectors)
        logger.debug('Registered signals for graceful shutdown: %s', signals)

    api.add_resource(CollectorsApi, '/<string:trigger_type>')

    logger.info('[OK] Collectors Microservice ready for incoming requests')


if __name__ in ('__main__', 'start'):
    main()
