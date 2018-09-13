import sys
import fiona

from smfrcore.models.sql import Nuts2, create_app
from scripts.utils import ParserHelpOnError


def add_args(parser):
    parser.add_argument('-f', '--shapefile', help='Shapefile reference', type=str,
                        metavar='shapefile', required=True)


def main():
    parser = ParserHelpOnError(description='Build Nuts2 JSON out of a shapefile for import during DB migration')
    add_args(parser)
    conf = parser.parse_args()
    flask_app = create_app()
    flask_app.app_context().push()

    with fiona.open(conf.shapefile) as shp:
        for f in shp:
            efas_id = f['id']
            nuts2_id = f['properties']['ID']
            nuts2 = Nuts2.query.get(nuts2_id)
            print('Updating efas id from', nuts2.efas_id, 'to', efas_id)
            nuts2.efas_id = efas_id
            nuts2.save()
    print('finished')


if __name__ == '__main__':
    sys.exit(main())
