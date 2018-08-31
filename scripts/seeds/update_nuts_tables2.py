import os
import tarfile

import ujson
import unicodedata

from smfrcore.models.sqlmodels import Nuts2, Nuts3, create_app


if __name__ == '__main__':
    pathnuts2 = os.path.join(os.path.dirname(__file__), 'data/eurostat_nuts2.json.tar.gz')
    pathnuts3 = os.path.join(os.path.dirname(__file__), 'data/eurostat_nuts3.json.tar.gz')
    app = create_app()
    app.app_context().push()

    with tarfile.open(pathnuts2, 'r:gz') as tar:
        archive = tar.getmembers()[0]
        init_f = tar.extractfile(archive)
        data = ujson.load(init_f)

    for nutsid, nutsname in data.items():
        nutsname = unicodedata.normalize('NFKD', nutsname).encode('ascii', 'ignore').decode()

        nuts2list = list(Nuts2.query.filter_by(efas_name=nutsname)) + list(Nuts2.query.filter_by(nuts_id=nutsid))

        for nuts2 in nuts2list:
            nuts2.efas_name = nutsname
            nuts2.nuts_id = nutsid
            nuts2.save()

    with tarfile.open(pathnuts3, 'r:gz') as tar:
        archive = tar.getmembers()[0]
        init_f = tar.extractfile(archive)
        data = ujson.load(init_f)

    for nutsid, nutsname in data.items():
        nutsnameascii = unicodedata.normalize('NFKD', nutsname).encode('ascii', 'ignore').decode()

        nuts3list = list(Nuts3.query.filter_by(name_ascii=nutsnameascii)) + list(Nuts3.query.filter_by(nuts_id=nutsid))

        for nuts3 in nuts3list:
            nuts3.name_ascii = nutsnameascii
            nuts3.name = nutsname
            nuts3.nuts_id = nutsid
            nuts3.save()
