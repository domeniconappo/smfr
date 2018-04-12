from setuptools import setup, find_packages

setup(
    name='smfr_core',
    version='1.0',
    packages=find_packages(),
    install_requires=['sqlalchemy_utils', 'flask', 'flask-sqlalchemy', 'flask-cqlalchemy'],
)
