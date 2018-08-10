import datetime

from smfrcore.models.sqlmodels import TwitterCollection

from daemons.collector import Collector


def add_collection_helper(**data):
    collection = TwitterCollection.create(**data)
    if collection.runtime:
        collector = Collector(collection, timezone=data.get('timezone', '+00:00'))
        collector.launch()
    return collection


def runtime_from_leadtime(lead_time):
    """

    :param lead_time: number of days before the peak occurs
    :return: runtime in format %Y-%m-%d %H:%M
    """
    runtime = datetime.datetime.now() + datetime.timedelta(days=int(lead_time) + 2)
    return runtime.strftime('%Y-%m-%d %H:%M')


def add_collection_from_rra_event(**event):
    runtime = runtime_from_leadtime(event['lead_time'])
    data = {'trigger': event['trigger'], 'runtime': runtime, 'locations': event['bbox'],
            'timezone': event.get('tzclient', '+00:00'), 'keywords': event['keywords'],
            'nuts2': event['efas_id'], 'forecast_id': int(event['forecast'])}
    collection = add_collection_helper(**data)
    return collection
