Zookeeper prototype
===================

The goal of this effort is to play with Apache Zookeeper and see how well it
works with Python and gevent. This is largely hacked together code.

Instructions
------------

A localhost Zookeeper is expected. 

1. Create a virtualenv and source it::

    $ virtualenv ~/zkproto_ve
    $ . ~/zkproto_ve/bin/activate


2. Install zkproto and dependencies into the virtualenv::
    
    $ python setup.py develop


3. supervisord is used to manage the worker processes. Start it up from the
   zkproto source directory::
    
    $ supervisord


4. Run the tests::

    $ zkproto-trials


5. When you are done, kill supervisord::

    $ supervisorctl shutdown



Lessons learned
---------------

Using zookeeper with gevent takes some effort. I had to write a partial
wrapper client that marshals completion and watcher events to the gevent
thread using an ``os.pipe()``. If we use Zookeeper for real, this client will
need to be fleshed out and finished. (zookeeper_gevent.py)

