import os
import tarfile

import ujson
import unicodedata

from sqlalchemy import or_

from smfrcore.models import Nuts2, Nuts3, create_app


if __name__ == '__main__':
    pathnuts2 = os.path.join(os.path.dirname(__file__), 'data/eurostat_nuts2.json.tar.gz')
    pathnuts3 = os.path.join(os.path.dirname(__file__), 'data/eurostat_nuts3.json.tar.gz')
    app = create_app()
    app.app_context().push()

    with tarfile.open(pathnuts2, 'r:gz') as tar:
        archive = tar.getmembers()[0]
        init_f = tar.extractfile(archive)
        data2 = ujson.load(init_f)

    for nutsid, nutsname in data2.items():
        nutsnameascii = str(unicodedata.normalize('NFKD', nutsname).encode('ascii', 'ignore').decode())
        nutsnameascii = nutsnameascii if nutsnameascii.replace(', ', '').strip() else nutsname
        nuts2list = Nuts2.query.filter(or_(
            Nuts2.efas_name == nutsnameascii,
            Nuts2.nuts_id == nutsid
        ))
        print('*** nuts2 ', nutsid, nutsnameascii)
        for nuts2 in nuts2list:
            print('found ', str(nuts2))
            nuts2.efas_name = nutsnameascii
            nuts2.nuts_id = nutsid
            nuts2.save()

    with tarfile.open(pathnuts3, 'r:gz') as tar:
        archive = tar.getmembers()[0]
        init_f = tar.extractfile(archive)
        data3 = ujson.load(init_f)

    for nutsid, nutsname in data3.items():
        nutsnameascii = str(unicodedata.normalize('NFKD', nutsname).encode('ascii', 'ignore').decode())
        if nutsnameascii.replace(', ', '').strip():
            clause = or_(
                Nuts3.name_ascii == nutsnameascii,
                Nuts3.name == nutsname,
                Nuts3.nuts_id == nutsid
            )
        else:
            clause = or_(
                Nuts3.name == nutsname,
                Nuts3.nuts_id == nutsid
            )

        nuts3list = Nuts3.query.filter(clause)

        print('*** nuts3 ', nutsid, nutsname)
        for nuts3 in nuts3list:
            print('found ', str(nuts3))
            if nutsnameascii:
                nuts3.name_ascii = nutsnameascii
            nuts3.name = nutsname
            nuts3.nuts_id = nutsid
            nuts3.save()
