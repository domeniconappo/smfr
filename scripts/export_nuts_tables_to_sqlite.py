import os
import sys
import sqlite3
import tarfile

import ujson as json

from scripts.utils import ParserHelpOnError, import_env


nuts2_schema = """
create table nuts2
(
    id INTEGER not null primary key,
    efas_id INTEGER not null,
    efas_name varchar default NULL,
    nuts_id varchar,
    country varchar,
    geometry text,
    country_code varchar,
    min_lon float default NULL,
    max_lon float default NULL,
    min_lat float default NULL,
    max_lat float default NULL
);
"""

nuts2_index = """
create index nuts2_efas_id_index on nuts2 (efas_id);
"""
nuts2_fields = ('id', 'efas_id', 'efas_name', 'nuts_id', 'country', 'geometry',
                'country_code', 'min_lon', 'max_lon', 'min_lat', 'max_lat')

stmt_nuts2 = 'INSERT INTO nuts2 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'


nuts3_schema = """
create table nuts3
(
    id INTEGER not null primary key,
    efas_id INTEGER not null,
    name varchar default NULL,
    name_ascii varchar default NULL,
    latitude float not null,
    longitude float not null,
    names text not null,
    properties text not null,
    country_name varchar,
    nuts_id varchar default null,
    country_code varchar
);
"""

nuts3_index = """
create index nuts3_efas_id_index on nuts3 (efas_id);
"""

nuts3_fields = ('id', 'efas_id', 'name', 'name_ascii', 'latitude', 'longitude',
                'names', 'properties', 'country_name', 'nuts_id', 'country_code')

stmt_nuts3 = 'INSERT INTO nuts3 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'


def add_args(parser):
    parser.add_argument('-o', '--dbfile', help='Path to the sqlite DB file to create', type=str,
                        metavar='DB_FILE', required=False, default='./nuts.db')


def main():
    parser = ParserHelpOnError(description='Export Nuts tables to a SQLite file')
    add_args(parser)
    args = parser.parse_args()
    env = os.path.join(os.path.dirname(__file__), '../.env')
    import_env(env_file=env)
    if os.path.exists(args.dbfile):
        print('REMOVING EXISTING DB....', args.dbfile)
        os.remove(args.dbfile)
    lite_conn = sqlite3.connect(args.dbfile)
    lite_cursor = lite_conn.cursor()
    print('SQLITE CONNECTED!')
    lite_cursor.execute(nuts2_schema)
    lite_conn.commit()
    lite_cursor.execute(nuts2_index)
    lite_cursor.execute(nuts3_schema)
    lite_conn.commit()
    lite_cursor.execute(nuts3_index)
    lite_conn.commit()
    print('SCHEMAS CREATED!')

    path = os.path.join(os.path.dirname(__file__), '../rest_server/src/migrations/versions/smfr_nuts2.json.tar.gz')
    with tarfile.open(path, 'r:gz') as tar:
        archive = tar.getmembers()[0]
        init_f = tar.extractfile(archive)
        init_data = json.load(init_f, precise_float=True)

    for i, row_dict in enumerate(init_data):
        row = [row_dict[k] for k in nuts2_fields]
        print(i, row)
        lite_cursor.execute(stmt_nuts2, row)
    lite_conn.commit()
    print('POPULATED NUTS2')

    path = os.path.join(os.path.dirname(__file__), '../rest_server/src/migrations/versions/smfr_nuts3.json.tar.gz')
    with tarfile.open(path, 'r:gz') as tar:
        archive = tar.getmembers()[0]
        init_f = tar.extractfile(archive)
        init_data = json.load(init_f, precise_float=True)

    for i, row_dict in enumerate(init_data):
        row = [row_dict[k] for k in nuts3_fields]
        print(i, row)
        lite_cursor.execute(stmt_nuts3, row)
    lite_conn.commit()
    print('POPULATED NUTS3')

    lite_cursor.close()


if __name__ == '__main__':
    sys.exit(main())

