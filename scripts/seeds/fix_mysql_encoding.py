#! /usr/bin/env python
import os

import MySQLdb

from scripts.utils import import_env


if __name__ == '__main__':
    env = os.path.join(os.path.dirname(__file__), '../../.env')
    import_env(env_file=env)
    host = "127.0.0.1"
    user = 'root'
    passwd = os.environ['MYSQL_PASSWORD']
    dbname = os.environ['MYSQL_DBNAME']
    print('+connecting....')
    db = MySQLdb.connect(host=host, user=user, passwd=passwd, db=dbname)
    cursor = db.cursor()
    print('CONNECTED!')

    print('+altering DB....')
    cursor.execute("ALTER DATABASE `%s` CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_unicode_ci'" % dbname)
    print('DB ALTERED!')
    sql = "SELECT DISTINCT(table_name) FROM information_schema.columns WHERE table_schema = '%s'" % dbname
    cursor.execute(sql)

    results = cursor.fetchall()

    for row in results:
        print('+altering table %s' % row[0])
        cursor.execute("ALTER TABLE `%s` convert to character set utf8mb4 COLLATE utf8mb4_unicode_ci" % (row[0]))
        print('ALTERED table %s!!!' % row[0])
    db.commit()
    db.close()
