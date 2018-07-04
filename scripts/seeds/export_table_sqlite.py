import os
import sys
import sqlite3

import MySQLdb

from scripts.utils import ParserHelpOnError, import_env


def add_args(parser):
    parser.add_argument('-o', '--dbfile', help='Path to the sqlite DB file to create', type=str,
                        metavar='DB_FILE', required=False, default='./nuts.db')


def main():
    parser = ParserHelpOnError(description='Export Nuts tables to a SQLite file')
    add_args(parser)
    args = parser.parse_args()
    env = os.path.join(os.path.dirname(__file__), '../../.env')
    import_env(env_file=env)
    host = "127.0.0.1"
    user = 'root'
    passwd = os.environ['MYSQL_PASSWORD']
    dbname = os.environ['MYSQL_DBNAME']
    print('+connecting....')
    mysql_conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=dbname)
    mysql_cursor = mysql_conn.cursor()
    print('MYSQL CONNECTED!')
    lite_conn = sqlite3.connect(args.dbfile)
    lite_cursor = lite_conn.cursor()
    print('SQLITE CONNECTED!')

    mysql_cursor.execute('SELECT * from smfr.nuts2')
    for i, row in enumerate(mysql_cursor):
        stmt = 'INSERT INTO nuts2 VALUES {}'.format(str(row).replace('None', 'NULL'))
        print(i, row[0])
        lite_cursor.execute(stmt)
    lite_conn.commit()
    print('POPULATED NUTS2')
    mysql_cursor.execute('SELECT * from smfr.nuts3')
    for i, row in enumerate(mysql_cursor):
        stmt = 'INSERT INTO nuts3 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
        lite_cursor.execute(stmt, row)
    lite_conn.commit()
    print('POPULATED NUTS3')

    mysql_cursor.close()
    lite_cursor.close()


if __name__ == '__main__':
    sys.exit(main())

