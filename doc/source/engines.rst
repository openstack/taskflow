-------
Engines
-------

Overview
========

Engines are what **really** runs your tasks and flows.

An *engine* takes a flow structure (described by :doc:`patterns`) and uses it to
decide which :doc:`atom <atoms>` to run and when.

TaskFlow provides different implementation of engines. Some may be easier to
use (ie, require no additional infrastructure setup) and understand, others
might require more complicated setup but provide better scalability. The idea
and *ideal* is that deployers or developers of a service that uses TaskFlow can
select an engine that suites their setup best without modifying the code of
said service.

Engines might have different capabilities and configuration, but all of them
**must** implement same interface and preserve semantics of patterns (e.g.
parts of :py:class:`linear flow <taskflow.patterns.linear_flow.Flow>` are run
one after another, in order, even if engine is *capable* run tasks in
parallel).

Creating Engines
================

All engines are mere classes that implement same interface, and of course it is
possible to import them and create their instances just like with any classes
in Python. But easier (and recommended) way for creating engine is using
engine helpers. All of them are imported into `taskflow.engines` module, so the
typical usage of them might look like::

    from taskflow import engines

    ...
    flow = make_flow()
    engine = engines.load(flow, engine_conf=my_conf, backend=my_persistence_conf)
    engine.run


.. automodule:: taskflow.engines.helpers

Engine Configuration
====================

To select which engine to use and pass parameters to an engine you should use
``engine_conf`` parameter any helper factory function accepts. It may be:

* a string, naming engine type;
* a dictionary, holding engine type with key ``'engine'`` and possibly
  type-specific engine parameters.

Known engine types are listed below.

Single-Threaded Engine
----------------------

**Engine type**: ``'serial'``

Runs all tasks on the single thread -- the same thread `engine.run()` is called
on. This engine is used by default.


Parallel Engine
---------------

**Engine type**: ``'parallel'``

Parallel engine schedules tasks onto different threads to run them in parallel.

Additional configuration parameters:

* ``executor``: a class that provides ``concurrent.futures.Executor``-like
  interface; it will be used for scheduling tasks. You can use instances
  of ``concurrent.futures.ThreadPoolExecutor`` or
  ``taskflow.utils.eventlet_utils.GreenExecutor``. Sharing executor between
  engine instances provides better scalability.

.. note::
    Running tasks with ``concurrent.futures.ProcessPoolExecutor`` is not
    supported now.

Worker-Based Engine
-------------------

**Engine type**: ``'worker-based'``

This is engine that schedules tasks to **workers** -- separate processes
dedicated for tasks execution, possibly running on other machines.

This engine is under active development and is not recommended for production
use yet. For more information, please see `wiki page`_ for more details.

.. _wiki page: https://wiki.openstack.org/wiki/TaskFlow/Worker-based_Engine

Engine Interface
================

.. automodule:: taskflow.engines.base
