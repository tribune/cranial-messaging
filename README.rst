Cranial Messaging
====================

A universal pipe
----------------
Provides a utility that listens for messages at some URI and relays them to some target.

Try:
    ``$ cd cranial-messaging/bin/``
    
    ``$ echo "hello world" | ./cranial stdin:// file://./out.txt``
    
    ``$ ./cranial file://./out.txt stdout://``
    
    ``$ ./cranial --response --echo file://./out.txt http://httpbin.org/get``
    
    ``$ echo "- also means stdin" | ./cranial --response - httppost://httpbin.org/post``
    
    ``$./cranial kafka://broker.a,broker.b/topic # stdout is the default``
    
    ``$ ./cranial --json db://your.host/name/table?driver=psycopg2&id=id http://example.com/ # Coming soon.``
    
    ``$ ./cranial tweets://yourname:password@#someTag fb://yourname:password@ # Easy to implement.``
    
    ``$ ./cranial --help``



Distributed Application Tools
-----------------------------
#. "Messengers" (a.k.a Publishers) "Notifiers" (a.k.a. Transports) and
   "Listeners" (a.k.a. Subscribers) for asynchronous remote message passing,
   suitable for implementing Actor patterns.
#. Pluggable Service Discovery, initially implemented for Marathon, and a
   a desire to implement peer-to-peer gossip as a default mechanism.

Wrappers/Adapters for common services and protocols
---------------------------------------------------
#. HTTP
#. ZeroMQ
#. Kafka
#. Amazon Kinesis Firehose
#. Python DBAPI2 Databases
#. Celery (Incomplete & Deprecated in favor of Kafka)
#. Apache Mesos and Marathon


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

The Cranial Ontology is now formalized in OWL.
Canonical: http://ld.chapmanmedia.com/cranial
Github: https://github.com/tribune/cranial-common/blob/master/ontology/cranial


Contributing
============
Questions, Suggestions, Support requests, trouble reports, and of course, 
Pull Requests, are all welcome in the Github issue queue.
