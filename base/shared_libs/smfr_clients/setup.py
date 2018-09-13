from setuptools import setup, find_packages

version = open('VERSION').read().strip()
setup(
    name='smfrcore-clients',
    version=version,
    packages=find_packages(),
    description='SMFR Core modules (Clients)',
    author='Domenico Nappo',
    author_email='domenico.nappo@ext.ec.europa.eu',
    install_requires=['ujson', 'requests', 'Flask', 'PassLib', 'PyYAML', 'PyJWT', 'paramiko',
                      'flask-marshmallow', 'flask-jwt-extended', 'swagger-marshmallow-codegen', ],
)
