import signal

from daemons.consumer import Consumer
from daemons.collector import Collector


def stop_active_collectors():
    for _id, running_collector in Collector.running_instances().items():
        running_collector.stop(reanimate=True)
    Consumer.running_instance().stop()


signal.signal(signal.SIGINT, stop_active_collectors)
signal.signal(signal.SIGTERM, stop_active_collectors)
signal.signal(signal.SIGQUIT, stop_active_collectors)

__all__ = ['Consumer', 'Collector']
