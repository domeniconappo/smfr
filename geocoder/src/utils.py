from collections import namedtuple

import ujson as json


NutsItem = namedtuple('NutsItem', 'id, nuts_id, properties, geometry')


def read_geojson(path):
    items = []
    with open(path) as f:
        data = json.load(f, precise_float=True)
    for feat in data['features']:
        items.append(
            NutsItem(
                feat['properties']['ObjectID'],
                feat['properties']['NUTS_ID'],
                feat['properties'],
                feat['geometry']['coordinates']
            )
        )
    return items

