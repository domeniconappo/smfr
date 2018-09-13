from setuptools import setup, find_packages

version = open('VERSION').read().strip()
setup(
    name='smfrcore-utils',
    version=version,
    packages=find_packages(),
    description='SMFR Core modules (utilities)',
    author='Domenico Nappo',
    author_email='domenico.nappo@ext.ec.europa.eu',
    install_requires=['Flask', 'schedule',],
)
