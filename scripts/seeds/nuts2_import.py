import os
import sys

from collections import namedtuple
import ujson as json
import fiona

from smfrcore.models.sql import Nuts2
from scripts.utils import ParserHelpOnError


fields = ', '.join([c.name for c in Nuts2.__table__.columns])
Nuts2Item = namedtuple('Nuts2Item', fields)
current_dir = os.path.dirname(__file__)


def add_args(parser):
    parser.add_argument('-f', '--shapefile', help='Shapefile to import', type=str,
                        metavar='shapefile', required=True)
    parser.add_argument('-o', '--outfile', help='Path to output JSON', type=str,
                        metavar='outfile', required=False, default='nuts2.json')


def main():
    parser = ParserHelpOnError(description='Build Nuts2 JSON out of a shapefile for import during DB migration')

    add_args(parser)
    conf = parser.parse_args()
    res = []
    with open(os.path.join(current_dir, 'bbox_init.json')) as f:
        bb_content = json.load(f)
    bboxes = {e['id']: e for e in bb_content}
    with fiona.open(conf.shapefile) as shp:
        for f in shp:
            nuts2 = Nuts2.from_feature(f)
            bbox = bboxes.get(nuts2.id)
            if bbox:
                nuts2.min_lat = bbox['min_lat']
                nuts2.max_lat = bbox['max_lat']
                nuts2.min_lon = bbox['min_lon']
                nuts2.max_lon = bbox['max_lon']
            res.append(nuts2.to_dict())
            # print(nuts2.to_dict())
    with open(conf.outfile, 'w') as out_json:
        json.dump(res, out_json, ensure_ascii=False, indent=2)


if __name__ == '__main__':
    sys.exit(main())
