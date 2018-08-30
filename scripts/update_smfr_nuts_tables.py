import os
import sys
import tarfile

import ujson

from smfrcore.models.sqlmodels import Nuts2, Nuts3, create_app, sqldb


def load_json(table):
    path = os.path.join(os.path.dirname(__file__), 'seeds/data/smfr_{}.json.tar.gz'.format(table))
    with tarfile.open(path, 'r:gz') as tar:
        archive = tar.getmembers()[0]
        init_f = tar.extractfile(archive)
        data = ujson.load(init_f, precise_float=True)
    return data


def update_nutstables():
    app = create_app()
    app.app_context().push()
    conn = sqldb.engine.connect()
    conn.execute('SET FOREIGN_KEY_CHECKS = 0;')
    print(' ---------- UPGRADING nuts3')

    conn.execute('TRUNCATE TABLE nuts3')
    print('Deleted all rows in nuts3 table')
    print('loading nuts3 data...')

    data_nuts3 = load_json('nuts3')
    print('Bulk insert into Nuts3 table: %d rows' % len(data_nuts3))
    conn.execute(Nuts3.__table__.insert(), *data_nuts3)

    print(' ---------- UPGRADING nuts2')
    conn.execute('TRUNCATE TABLE nuts2')
    print('Deleted all rows in nuts2 table')

    print('loading nuts2 data...')
    data_nuts2 = load_json('nuts2')

    print('Bulk insert into Nuts2 table: %d rows' % len(data_nuts2))
    conn.execute(Nuts2.__table__.insert(), *data_nuts2)
    conn.execute('SET FOREIGN_KEY_CHECKS = 1;')
    print('[OK] Updated nuts2 and nuts3 tables.')


if __name__ == '__main__':
    sys.exit(update_nutstables())
