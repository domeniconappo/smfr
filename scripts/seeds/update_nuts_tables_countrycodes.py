import os
import sys
import tarfile

import ujson
from sqlalchemy import or_

from smfrcore.models import Nuts2, Nuts3, create_app


def _country_code2(cc3, country_value):
    if cc3 not in ('GRC', 'GBR'):
        return country_value['alpha_2']
    return 'EL' if cc3 == 'GRC' else 'UK'


def update():
    path = os.path.join(os.path.dirname(__file__), 'data/countries.json.tar.gz')
    with tarfile.open(path, 'r:gz') as tar:
        archive = tar.getmembers()[0]
        init_f = tar.extractfile(archive)
        data = ujson.load(init_f)

    app = create_app()
    app.app_context().push()

    for alpha3, v in data.items():
        print('***************************** country', v['name'])
        country_code = _country_code2(alpha3, v)
        nuts2list = list(Nuts2.query.filter(
            or_(Nuts2.country_code == country_code,
                Nuts2.country == v['name'])
        ))

        nuts3list = list(Nuts3.query.filter(
            or_(Nuts3.country_code == country_code,
                Nuts3.country_name == v['name'],
                Nuts3.nuts_id.like(country_code + '%'))
        ))

        print('***************************** NUTS2', v['name'])
        for nuts2 in nuts2list:
            nuts2.country_code3 = alpha3
            nuts2.country = v['name']
            nuts2.country_code = country_code
            nuts2.save()

        print('***************************** NUTS3', v['name'])
        for nuts3 in nuts3list:
            nuts3.country_code3 = alpha3
            nuts3.country_name = v['name']
            nuts3.country_code = country_code
            nuts3.save()

    return 0


if __name__ == '__main__':
    sys.exit(update())
