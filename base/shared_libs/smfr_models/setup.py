from setuptools import setup, find_packages

version = open('VERSION').read().strip()
packages = find_packages()
# input(packages)

setup(
    name='smfrcore-models',
    version=version,
    packages=packages,
    include_package_data=True,
    package_data={'smfrcore.models/data': ['data/*.gz']},
    # data_files=[('countries', ['data/countries.json.tar.gz'])],
    description='SMFR Core modules (models)',
    author='Domenico Nappo',
    author_email='domenico.nappo@ext.ec.europa.eu',
    install_requires=['ujson', 'requests', 'Flask', 'python-Levenshtein', 'fuzzywuzzy',
                      'sqlalchemy_utils', 'flask-sqlalchemy', 'flask-cqlalchemy',
                      'PassLib', 'PyYAML', 'PyJWT', 'shapely', 'fiona',
                      'flask-marshmallow', 'flask-jwt-extended', 'swagger-marshmallow-codegen'],
)
