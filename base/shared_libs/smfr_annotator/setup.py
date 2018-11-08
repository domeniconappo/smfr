from setuptools import setup, find_packages

version = open('VERSION').read().strip()
setup(
    name='smfrcore-annotator',
    version=version,
    packages=find_packages(),
    description='SMFR Core modules (annotator code)',
    author='Domenico Nappo',
    author_email='domenico.nappo@ext.ec.europa.eu',
    install_requires=['nltk~=3.1', 'Keras==2.2.2', 'numpy==1.14.5', 'scikit-learn', 'sklearn ~= 0.0', 'tensorflow==1.10.0'],
)
