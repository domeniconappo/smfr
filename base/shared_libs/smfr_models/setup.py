from setuptools import setup, find_packages

version = open('VERSION').read().strip()

setup(
    name='smfrcore-models',
    version=version,
    packages=find_packages(),
    description='SMFR Core modules (models)',
    author='Domenico Nappo',
    include_package_data=True,
    author_email='domenico.nappo@ext.ec.europa.eu',
    install_requires=['ujson', 'requests', 'Flask', 'python-Levenshtein', 'fuzzywuzzy',
                      'sqlalchemy_utils', 'flask-sqlalchemy', 'flask-cqlalchemy',
                      'PassLib', 'PyYAML', 'PyJWT',
                      'flask-marshmallow', 'flask-jwt-extended', 'swagger-marshmallow-codegen'],
)
