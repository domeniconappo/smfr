import os
import sys

from collections import namedtuple
import ujson as json
import fiona

from smfrcore.models.sqlmodels import Nuts3
from scripts.utils import ParserHelpOnError


fields = ', '.join([c.name for c in Nuts3.__table__.columns])
Nuts3Item = namedtuple('Nuts3Item', fields)
current_dir = os.path.dirname(__file__)


def add_args(parser):
    parser.add_argument('-f', '--shapefile', help='Shapefile to import', type=str,
                        metavar='shapefile', required=True)
    parser.add_argument('-o', '--outfile', help='Path to output JSON', type=str,
                        metavar='outfile', required=False, default='nuts3.json')


def main(args):
    parser = ParserHelpOnError(description='Build Nuts3 JSON out of a shapefile for import during DB migration')

    add_args(parser)
    conf = parser.parse_args(args)
    res = []
    with open(os.path.join(current_dir, 'nuts2_join_efas_id.json')) as f:
        nuts2 = json.load(f)
    nuts2s = {e['id']: e['efas_id'] for e in nuts2}
    with fiona.open(conf.shapefile) as shp:
        for f in shp:
            nuts3 = Nuts3.from_feature(f)
            efas_id = nuts2s[nuts3.join_id]
            nuts3.efas_id = efas_id
            res.append(nuts3.to_dict())
    with open(conf.outfile, 'w') as out_json:
        json.dump(res, out_json, ensure_ascii=False, indent=2)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
