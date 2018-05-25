from collections import namedtuple

import ujson as json


class NutsItem(namedtuple('NutsItem', 'id, nuts_id, properties, geometry')):
    def __eq__(self, other):
        return self.id == other.id


def read_geojson(path):
    items = []
    try:
        with open(path) as f:
            data = json.load(f, precise_float=True)
    except FileNotFoundError:
        data = {'features': []}
        print('File not found: ', path)

    for feat in data['features']:
        properties = feat['properties']
        geometry = feat['geometry']
        items.append(
            NutsItem(
                properties['ObjectID'],
                properties['NUTS_ID'],
                properties,
                geometry['coordinates']
            )
        )
    return items
