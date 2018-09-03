import os
import sys
import tarfile
import datetime

import json

from smfrcore.models.sqlmodels import Nuts2, Nuts3, create_app

app = create_app()
app.app_context().push()


def make_tarfile(output_filename, source_dir):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))


def export():
    current_export_date = datetime.datetime.now().strftime('%Y%m%d%H%M')
    pathnuts2 = os.path.join(os.path.dirname(__file__), 'data/{}_smfr_nuts2.json'.format(current_export_date))
    pathnuts3 = os.path.join(os.path.dirname(__file__), 'data/{}_smfr_nuts3.json'.format(current_export_date))

    nuts2 = [n.__dict__ for n in Nuts2.query.all()]
    for n in nuts2:
        n.pop('_sa_instance_state')
    with open(pathnuts2, 'w') as fp:
        json.dump(nuts2, fp, indent=2, ensure_ascii=False)
    make_tarfile(pathnuts2 + '.tar.gz', pathnuts2)

    nuts3 = [n.__dict__ for n in Nuts3.query.all()]
    for n in nuts3:
        n.pop('_sa_instance_state')
    with open(pathnuts3, 'w') as fp:
        json.dump(nuts3, fp, indent=2, ensure_ascii=False)
    make_tarfile(pathnuts3 + '.tar.gz', pathnuts3)

    os.unlink(pathnuts2)
    os.unlink(pathnuts3)


if __name__ == '__main__':
    sys.exit(export())
