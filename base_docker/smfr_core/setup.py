from setuptools import setup, find_packages

setup(
    name='smfrcore',
    version='1.1',
    packages=find_packages(),
    description='SMFR Core modules (models, utilities)',
    author='Domenico Nappo',
    author_email='domenico.nappo@ext.ec.europa.eu',
    install_requires=['ujson', 'requests', 'Flask', 'sqlalchemy_utils', 'flask-sqlalchemy', 'flask-cqlalchemy'],
)
