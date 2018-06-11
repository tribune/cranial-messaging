#!/usr/bin/env python
from os import path
from setuptools import setup, find_packages

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()


setup(name='cranial-messaging',
      version='0.2.0',
      namespace_packages=['cranial'],
      description='Cranial Messaging',
      long_description=long_description,
      author='Tronc Data Team',
      author_email='matt@ninjitsuweb.com',
      url='https://github.com/tribune/cranial-messaging',
      packages=find_packages(exclude=['tests*']),
      install_requires=['arrow>=0.10.0',
                        'boto3',
                        'cachetools',
                        'cranial-common',
                        'psycopg2>=2.6.2',
                        'requests-futures==0.9.7',
                        'zmq'],
      extras_require={}
      )
