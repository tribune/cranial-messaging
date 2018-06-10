#!/usr/bin/env python
from os import path
from setuptools import setup, find_packages

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()


setup(name='cranial-datastore',
      version='0.2.0',
      description='Cranial Datastore',
      long_description=long_description,
      author='Tronc Data Team',
      author_email='matt@ninjitsuweb.com',
      url='https://github.com/tribune/cranial-datastore',
      packages=find_packages(exclude=['tests*']),
      install_requires=['boto3',
                        'cachetools',
                        'cranial-common'],
      extras_require={}
      )
