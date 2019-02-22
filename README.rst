Cranial Datastore
======================

Sub-components:

Adapters: Wrappers to standardize interfaces to datastores.

Fetchers: Utilities for transferring Bytes or Records between different
datastores, or between datastores and services.

KeyValue: A Dict-like interface to DBAPI2 and other datastores.


About Cranial
======================

Cranial is a Framework and Toolkit for building distributed applications and
microservices in Python, with a particular focus on services delivering
predictions from online learning models.

The machine learning components do not provide algorithms or models like
SciKitLearn or Tensorflow or Spark or H2O, but instead provide wrappers so that
models and pipelines created by these tools can be deployed and combined in
standardized ways.

A slide deck with detailed diagrams of Cranial architecture can be found here:
https://docs.google.com/presentation/d/131RK79w-Ls7uKuQocDcyEBXWDWABv6fXpaK_1THBG2Y/edit?usp=sharing

The Craial Ontology is now formalized in OWL.
Canonical: http://ld.chapmanmedia.com/cranial
Github: https://github.com/tribune/cranial-common/blob/master/ontology/cranial


Contributing
============
Questions, Suggestions, Support requests, trouble reports, and of course, 
Pull Requests, are all welcome in the Github issue queue.
