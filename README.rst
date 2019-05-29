Cranial Messaging
====================

Installation
------------
The PyPI packages are probably out-of-date. Install from Github to be able to
run examples in this file.


What is it? A Library...
---------
Modules for reading and writing data from various sources, built
with a "streaming-first" approach.

What is it? A universal pipe application...
----------------
Provides a utility that listens for messages at some URI and relays them to
some target. Currently optimized for user convenience and development speed
over run-time performance.

Try:
    ``$ cd cranial-messaging/bin/``

    ``$ echo "hello world" | ./cranial stdin:// file://./out.txt``

    ``$ ./cranial pipe file://./out.txt stdout://``

    ``$ ./cranial pipe --response --echo file://./out.txt http://httpbin.org/anything``

    ``$ echo "- also means stdin" | ./cranial pipe --response - httppost://httpbin.org/post``

    ``$./cranial pipe kafka://broker.a,broker.b/topic # stdout is the default``

    | ``$ ./cranial pipe postgresql://your.host:5439/name/table?last_id=0 \``
    | ``>  ssh://you@example.com:22022/file.json.bzip2``

    | ``$ ./cranial pipe db://your.host/name/table?driver=mysql \``
    | ``>  hdfs://example.com/path/to/file.json.gz``

    | ``$ ./cranial pipe tweets://yourname:password@#someTag \``
    | ``>   fb://yourname:password@ # Doesn't exist yet, but Easy to implement.``

    | ``$ ./cranial pipe --response out.txt http://httpbin.org/anything \``
    | ``>  | ./cranial pipe - s3://bucket/piping-to-myself/responses.txt.gz``

    ``$ ./cranial pipe --list  # Get supported protocols``

    ``$ ./cranial pipe --help``



Distributed Application Tools
-----------------------------
#. "Messengers" (a.k.a Publishers) "Notifiers" (a.k.a. Transports) and
   "Listeners" (a.k.a. Subscribers) for asynchronous remote message passing,
   suitable for implementing Actor & Enterprise Integration Patterns.

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


Some Candidates for Future modules?
-----------------------------------
Contributions welcome!
#. Logstash
#. Redis

About Cranial
======================

Cranial is a Framework and Toolkit for building distributed applications and
microservices in Python, with a "streaming-first" approach to data pipelines,
and built especially for services delivering predictions from online learning
models, with a hope to be useful to many kinds of applications.

The machine learning components do not provide algorithms or models like
SciKitLearn or Tensorflow or Spark or H2O, but instead provide wrappers so that
models and pipelines created by these tools can be deployed and combined in
standardized ways.

A slide deck with detailed diagrams of Cranial architecture can be found here:
https://docs.google.com/presentation/d/131RK79w-Ls7uKuQocDcyEBXWDWABv6fXpaK_1THBG2Y/edit?usp=sharing

Learn about Enterprise Integration Patterns here:
https://www.enterpriseintegrationpatterns.com/patterns/messaging/Chapter1.html

The Cranial Ontology is now formalized in OWL.
Canonical: http://ld.chapmanmedia.com/cranial
Github: https://github.com/tribune/cranial-common/blob/master/ontology/cranial


Contributing
============
Questions, Suggestions, Support requests, trouble reports, and of course,
Pull Requests, are all welcome in the Github issue queue.
