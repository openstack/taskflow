-------
Engines
-------

Overview
========

Engines are what **really** runs your atoms.

An *engine* takes a flow structure (described by :doc:`patterns`) and uses it to
decide which :doc:`atom <atoms>` to run and when.

TaskFlow provides different implementations of engines. Some may be easier to
use (ie, require no additional infrastructure setup) and understand; others
might require more complicated setup but provide better scalability. The idea
and *ideal* is that deployers or developers of a service that uses TaskFlow can
select an engine that suites their setup best without modifying the code of
said service.

Engines usually have different capabilities and configuration, but all of them
**must** implement the same interface and preserve the semantics of patterns (e.g.
parts of :py:class:`linear flow <taskflow.patterns.linear_flow.Flow>` are run
one after another, in order, even if engine is *capable* of running tasks in
parallel).

Creating
========

All engines are mere classes that implement the same interface, and of course
it is possible to import them and create instances just like with any classes
in Python. But the easier (and recommended) way for creating an engine is using
the engine helper functions. All of these functions are imported into the
`taskflow.engines` module namespace, so the typical usage of these functions
might look like::

    from taskflow import engines

    ...
    flow = make_flow()
    engine = engines.load(flow, engine_conf=my_conf, backend=my_persistence_conf)
    engine.run


.. automodule:: taskflow.engines.helpers

Usage
=====

To select which engine to use and pass parameters to an engine you should use
the ``engine_conf`` parameter any helper factory function accepts. It may be:

* a string, naming engine type;
* a dictionary, holding engine type with key ``'engine'`` and possibly
  type-specific engine parameters.

Single-Threaded
---------------

**Engine type**: ``'serial'``

Runs all tasks on the single thread -- the same thread `engine.run()` is called
on. This engine is used by default.

.. tip::

    If eventlet is used then this engine will not block other threads
    from running as eventlet automatically creates a co-routine system (using
    greenthreads and monkey patching). See `eventlet <http://eventlet.net/>`_
    and `greenlet <http://greenlet.readthedocs.org/>`_ for more details.

Parallel
--------

**Engine type**: ``'parallel'``

Parallel engine schedules tasks onto different threads to run them in parallel.

Additional configuration parameters:

* ``executor``: a class that provides ``concurrent.futures.Executor``-like
  interface; it will be used for scheduling tasks. You can use instances
  of ``concurrent.futures.ThreadPoolExecutor`` or
  ``taskflow.utils.eventlet_utils.GreenExecutor`` (which internally uses
  `eventlet <http://eventlet.net/>`_ and greenthread pools).

.. tip::

    Sharing executor between engine instances provides better
    scalability by reducing thread creation and teardown as well as by reusing
    existing pools (which is a good practice in general).

.. note::

    Running tasks with ``concurrent.futures.ProcessPoolExecutor`` is not
    supported now.

Worker-Based
------------

**Engine type**: ``'worker-based'``

For more information, please see :doc:`workers <workers>` for more details on
how the worker based engine operates (and the design decisions behind it).

Interfaces
==========

.. automodule:: taskflow.engines.base

Hierarchy
=========

.. inheritance-diagram::
    taskflow.engines.base
    taskflow.engines.action_engine.engine
    taskflow.engines.worker_based.engine
    :parts: 1
