import os
import tarfile

import ujson
from smfrcore.models.sqlmodels import Nuts2, Nuts3, create_app


def update_nutstables_8163c029df4e():

    path = os.path.join(os.path.dirname(__file__), 'countries.json.tar.gz')
    with tarfile.open(path, 'r:gz') as tar:
        archive = tar.getmembers()[0]
        init_f = tar.extractfile(archive)
        data = ujson.load(init_f)

    app = create_app()
    app.app_context().push()

    for alpha3, v in data.items():
        print('***************************** nuts2 country', v['name'])
        nuts2list = list(Nuts2.query.filter_by(country_code=v['alpha_2'])) + list(Nuts2.query.filter_by(country=v['name']))

        if alpha3 == 'GRC':
            nuts2list += list(Nuts2.query.filter_by(country_code='EL'))
            country_code = 'EL'
        elif alpha3 == 'GBR':
            nuts2list += list(Nuts2.query.filter_by(country_code='UK'))
            country_code = 'UK'
        else:
            country_code = v['alpha_2']

        if not nuts2list:
            print('No NUTS2 items for country', v['name'])
            continue

        for nuts2 in nuts2list:
            nuts2.country_code3 = alpha3
            nuts2.country = v['name']
            nuts2.country_code = country_code
            nuts2.save()

    for alpha3, v in data.items():
        print('++++++++++++++++++++++++ nuts3 country', v['name'])
        nuts3list = list(Nuts3.query.filter_by(country_code=v['alpha_2'])) \
                    + list(Nuts3.query.filter_by(country_name=v['name'])) \

        if alpha3 == 'GRC':
            nuts3list += list(Nuts3.query.filter_by(country_code='EL')) + list(Nuts3.query.filter(Nuts3.nuts_id.like('EL%')))
            country_code = 'EL'
        elif alpha3 == 'GBR':
            nuts3list += list(Nuts3.query.filter_by(country_code='UK')) + list(Nuts3.query.filter(Nuts3.nuts_id.like('UK%')))
            country_code = 'UK'
        else:
            nuts3list += list(Nuts3.query.filter(Nuts3.nuts_id.like(v['alpha_2'] + '%')))
            country_code = v['alpha_2']

        if not nuts3list:
            print('No NUTS3 items for country', v['name'])
            continue

        for nuts3 in nuts3list:
            nuts3.country_code3 = alpha3
            nuts3.country_name = v['name']
            nuts3.country_code = country_code
            nuts3.save()
