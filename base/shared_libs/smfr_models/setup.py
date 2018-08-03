from setuptools import setup, find_packages

setup(
    name='smfrcore-models',
    version='1.5',
    packages=find_packages(),
    description='SMFR Core modules (models)',
    author='Domenico Nappo',
    author_email='domenico.nappo@ext.ec.europa.eu',
    install_requires=['ujson', 'requests', 'Flask',
                      'sqlalchemy_utils', 'flask-sqlalchemy', 'flask-cqlalchemy',
                      'PassLib', 'PyYAML', 'PyJWT',
                      'flask-marshmallow', 'flask-jwt-extended', 'swagger-marshmallow-codegen'],
)
