from setuptools import setup, find_packages

version = open('VERSION').read().strip()
setup(
    name='smfrcore-analysis',
    version=version,
    packages=find_packages(),
    description='SMFR Core modules (annotator and geocoder)',
    author='Domenico Nappo',
    author_email='domenico.nappo@ext.ec.europa.eu',
    install_requires=['nltk~=3.1', 'Keras==2.2.4', 'numpy>=1.15.0', 'scikit-learn', 'sklearn ~= 0.0',
                      'tensorflow==1.12.0'],
)
