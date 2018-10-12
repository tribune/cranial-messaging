Messenger
=========

Messenger is a framework for sending synchronous or
asynchronous messages between microservices, and automatically supports
Marathon's REST API for service discovery.

Inspired in no small part by:
    https://blogs.mulesoft.com/dev/connectivity-dev/why-messaging-queues-suck/

To introduce the terminology:
    - A 'producer' uses a Messenger object to broadcast a 'message' (string)
      with a certain 'Label' (a.k.a "topic") to self-registered 'consumers.'
    - A Messenger uses various Notifiers to deliver the message to each consumer
      service.
    - A consumer service specifies (using Marathon Service Labels, or
      user-specified functions):
      - the 'Label' of messages it wishes to receive,
      - whether those messages should go to 'all' tasks for the service, or
      'any' task (a.k.a instance), and
      - which type of Notifier to use.
    - Beside the message itself, a Notifier is provided an 'address' by
      ServiceDiscovery, and an 'endpoint' by the Messenger.

Notifiers exists for:

=========   ===========   ==================   ==========
Notifier    Address       Endpoint             Note
=========   ===========   ==================   ==========
HTTP        Hostname      URL Path
Kafka       (ignored)     Topic
ZMQ         Hostname      (ignored)
Celery      Broker Host   Channel              Deprecated
LocalDisk   File path     (ignored)
Firehose    (ignored)     DeliveryStreamName
=========   ===========   ==================   ==========


Basic library usage, assuming you have an HTTP server listening for a
<message> at /key/<message> and it has DCOS labels
'FOO' = ('any'|'all') and 'NOTIFIER' = 'http':

    from cranial.messaging import Messenger
    Messenger(label='FOO').notify(string)

It will raise an Exception if any messages fail. You can do
fancier things by implementing your own child of class Messenger.

F.A.Q.
======
Q: How do I use Messenger to send data to a HTTP/ZMQ/Kafka/etc service?

A: This is largely the wrong question to ask. An application that wants to send
data shouldn't need to know anything about the interfaces used by the consumers
of that data. It's the consumer's job to declare the protocol that they speak,
and to understand the structuring of the data that can be sent. The purpose of
the Messenger object is to abstract away these details from the concerns of the
developer of an application that produces data that may be of interest to other
applications.

A Messenger publishes data with one "Label" to potentially many "Endpoints."
That is, a consumer of Messenger data can subscribe to a Label, and should
implement endpoints for each of the types of messages the Label might contain.
Thus the consumer needs to understand the internals of the producer to some
extent, but the producer does not need to understand any internals of the
consumer. This is an intentional design choice.

Literal use of Messenger is `Messenger(label=required_label,
endpoint=optional_endpoint).notify(some_bytes)`. However, applications are
encouraged to instantiate Messenger once and re-use it for subsequent calls to
`notify()`, since this allows Notifiers (the internal delivery agents) to
optimize for re-use.

If an 'endpoint' is not specified, a default of "key" will be provided to
consumers that require it.

Generally, consumers of Messenger data should use a Listener class object to
receive the data. But since HTTP is a supported protocol, a simple flask
application could be used to provide the required endpoints.

Note that if you don't want the abstraction provided by Messenger, it's
perfectly reasonable to use any of it's Notifier classes directly. This might
be desirable, for example, to optimize performance if you are using low
performance service discovery. The current Marathon service discovery, in fact,
seems not to be be highly efficient for large scale messaging. Ideally,
however, this would resolved by a better service discovery implementation, not
by avoiding Messenger.


Roadmap
=======

- Rename 'endpoint' for better clarity? Maybe 'channel' ?

- Make 'endpoint' and argument to `notify()` instead of `__init__` so the same
  Messenger can send to multiple endpoints.

- Relocate Notifiers in their own files.

- Refactor Service Discovery for better abstraction to other solutions.

- Implement a static config ServiceDiscovery class.

- Finishing re-factoring `notify()` as demonstrated in `new_notify()`.
