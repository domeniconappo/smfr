import csv
import sys
import os
import chardet

from smfrcore.models.sql import Nuts2, create_app

app = create_app()
app.app_context().push()


def do():
    # csv format
    # FID, COUNTRY, ISO_CODE, ISO_CC, ISO_SUB, NUTS_ID, CNTR_CODE, EFAS_name, ID
    # 1403,,,,,ES111,ES,A Coruna,1410

    pathcsv = os.path.join(os.path.dirname(__file__), 'data/official_efas_nuts.csv')
    with open(pathcsv, 'rb') as f:
        result = chardet.detect(f.read())  # or readline if the file is large
    input(result['encoding'])

    with open(pathcsv, encoding=result['encoding']) as csvfile:
        reader = csv.DictReader(csvfile)
        for i, row in enumerate(reader, start=2):
            country_code = row['ISO_CC'].strip() or row['CNTR_CODE'].strip()
            nuts_id = row['NUTS_ID'].strip()
            country = row['COUNTRY'].strip()
            print(i, ') name:', row['EFAS_name'], 'nuts_id:', nuts_id, 'efas_id:', row['FID'], 'ID:', row['ID'], 'country code:', country_code)
            nuts2 = Nuts2.query.get(int(row['ID'].strip()))
            nuts2.efas_id = int(row['FID'].strip())
            if not nuts2.country_code:
                nuts2.country_code = country_code
            if not nuts2.country:
                nuts2.country = country
            if not nuts2.nuts_id:
                nuts2.nuts_id = nuts_id
            nuts2.save()


if __name__ == '__main__':
    sys.exit(do())
