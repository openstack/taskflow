-------
Engines
-------

Overview
========

Engines are what **really** runs your atoms.

An *engine* takes a flow structure (described by :doc:`patterns <patterns>`)
and uses it to decide which :doc:`atom <atoms>` to run and when.

TaskFlow provides different implementations of engines. Some may be easier to
use (ie, require no additional infrastructure setup) and understand; others
might require more complicated setup but provide better scalability. The idea
and *ideal* is that deployers or developers of a service that use TaskFlow can
select an engine that suites their setup best without modifying the code of
said service.

.. note::

    Engines usually have different capabilities and configuration, but all of
    them **must** implement the same interface and preserve the semantics of
    patterns (e.g. parts of a :py:class:`.linear_flow.Flow`
    are run one after another, in order, even if the selected
    engine is *capable* of running tasks in parallel).

Why they exist
--------------

An engine being *the* core component which actually makes your flows progress
is likely a new concept for many programmers so let's describe how it operates
in more depth and some of the reasoning behind why it exists. This will
hopefully make it more clear on their value add to the TaskFlow library user.

First though let us discuss something most are familiar already with; the
difference between `declarative`_ and `imperative`_ programming models. The
imperative model involves establishing statements that accomplish a programs
action (likely using conditionals and such other language features to do this).
This kind of program embeds the *how* to accomplish a goal while also defining
*what* the goal actually is (and the state of this is maintained in memory or
on the stack while these statements execute). In contrast there is the
declarative model which instead of combining the *how* to accomplish a goal
along side the *what* is to be accomplished splits these two into only
declaring what the intended goal is and not the *how*. In TaskFlow terminology
the *what* is the structure of your flows and the tasks and other atoms you
have inside those flows, but the *how* is not defined (the line becomes blurred
since tasks themselves contain imperative code, but for now consider a task as
more of a *pure* function that executes, reverts and may require inputs and
provide outputs). This is where engines get involved; they do the execution of
the *what* defined via :doc:`atoms <atoms>`, tasks, flows and the relationships
defined there-in and execute these in a well-defined manner (and the engine is
responsible for any state manipulation instead).

This mix of imperative and declarative (with a stronger emphasis on the
declarative model) allows for the following functionality to become possible:

* Enhancing reliability: Decoupling of state alterations from what should be
  accomplished allows for a *natural* way of resuming by allowing the engine to
  track the current state and know at which point a workflow is in and how to
  get back into that state when resumption occurs.
* Enhancing scalability: When an engine is responsible for executing your
  desired work it becomes possible to alter the *how* in the future by creating
  new types of execution backends (for example the `worker`_ model which does
  not execute locally). Without the decoupling of the *what* and the *how* it
  is not possible to provide such a feature (since by the very nature of that
  coupling this kind of functionality is inherently very hard to provide).
* Enhancing consistency: Since the engine is responsible for executing atoms
  and the associated workflow, it can be one (if not the only) of the primary
  entities that is working to keep the execution model in a consistent state.
  Coupled with atoms which *should* be immutable and have have limited (if any)
  internal state the ability to reason about and obtain consistency can be
  vastly improved.

  * With future features around locking (using `tooz`_ to help) engines can
    also help ensure that resources being accessed by tasks are reliably
    obtained and mutated on. This will help ensure that other processes,
    threads, or other types of entities are also not executing tasks that
    manipulate those same resources (further increasing consistency).

Of course these kind of features can come with some drawbacks:

* The downside of decoupling the *how* and the *what* is that the imperative
  model where functions control & manipulate state must start to be shifted
  away from (and this is likely a mindset change for programmers used to the
  imperative model). We have worked to make this less of a concern by creating
  and encouraging the usage of :doc:`persistence <persistence>`, to help make
  it possible to have state and transfer that state via a argument input and
  output mechanism.
* Depending on how much imperative code exists (and state inside that code)
  there *may* be *significant* rework of that code and converting or
  refactoring it to these new concepts. We have tried to help here by allowing
  you to have tasks that internally use regular python code (and internally can
  be written in an imperative style) as well as by providing
  :doc:`examples <examples>` that show how to use these concepts.
* Another one of the downsides of decoupling the *what* from the *how*  is that
  it may become harder to use traditional techniques to debug failures
  (especially if remote workers are involved). We try to help here by making it
  easy to track, monitor and introspect the actions & state changes that are
  occurring inside an engine (see :doc:`notifications <notifications>` for how
  to use some of these capabilities).

.. _declarative: http://en.wikipedia.org/wiki/Declarative_programming
.. _imperative: http://en.wikipedia.org/wiki/Imperative_programming
.. _tooz: https://github.com/openstack/tooz

Creating
========

.. _creating engines:

All engines are mere classes that implement the same interface, and of course
it is possible to import them and create instances just like with any classes
in Python. But the easier (and recommended) way for creating an engine is using
the engine helper functions. All of these functions are imported into the
``taskflow.engines`` module namespace, so the typical usage of these functions
might look like::

    from taskflow import engines

    ...
    flow = make_flow()
    eng = engines.load(flow, engine='serial', backend=my_persistence_conf)
    eng.run()
    ...


.. automodule:: taskflow.engines.helpers

Usage
=====

To select which engine to use and pass parameters to an engine you should use
the ``engine`` parameter any engine helper function accepts and for any engine
specific options use the ``kwargs`` parameter.

Types
=====

Serial
------

**Engine type**: ``'serial'``

Runs all tasks on a single thread -- the same thread
:py:meth:`~taskflow.engines.base.Engine.run` is called from.

.. note::

    This engine is used by **default**.

.. tip::

    If eventlet is used then this engine will not block other threads
    from running as eventlet automatically creates a implicit co-routine
    system (using greenthreads and monkey patching). See
    `eventlet <http://eventlet.net/>`_ and
    `greenlet <http://greenlet.readthedocs.org/>`_ for more details.

Parallel
--------

**Engine type**: ``'parallel'``

A parallel engine schedules tasks onto different threads/processes to allow for
running non-dependent tasks simultaneously. See the documentation of
:py:class:`~taskflow.engines.action_engine.engine.ParallelActionEngine` for
supported arguments that can be used to construct a parallel engine that runs
using your desired execution model.

.. tip::

    Sharing an executor between engine instances provides better
    scalability by reducing thread/process creation and teardown as well as by
    reusing existing pools (which is a good practice in general).

.. warning::

    Running tasks with a `process pool executor`_ is **experimentally**
    supported. This is mainly due to the `futures backport`_ and
    the `multiprocessing`_ module that exist in older versions of python not
    being as up to date (with important fixes such as :pybug:`4892`,
    :pybug:`6721`, :pybug:`9205`, :pybug:`16284`,
    :pybug:`22393` and others...) as the most recent python version (which
    themselves have a variety of ongoing/recent bugs).

Workers
-------

.. _worker:

**Engine type**: ``'worker-based'`` or ``'workers'``

.. note:: Since this engine is significantly more complicated (and
          different) then the others we thought it appropriate to devote a
          whole documentation :doc:`section <workers>` to it.

How they run
============

To provide a peek into the general process that an engine goes through when
running lets break it apart a little and describe what one of the engine types
does while executing (for this we will look into the
:py:class:`~taskflow.engines.action_engine.engine.ActionEngine` engine type).

Creation
--------

The first thing that occurs is that the user creates an engine for a given
flow, providing a flow detail (where results will be saved into a provided
:doc:`persistence <persistence>` backend). This is typically accomplished via
the methods described above in `creating engines`_. The engine at this point
now will have references to your flow and backends and other internal variables
are setup.

Compiling
---------

During this stage (see :py:func:`~taskflow.engines.base.Engine.compile`) the
flow will be converted into an internal graph representation using a
compiler (the default implementation for patterns is the
:py:class:`~taskflow.engines.action_engine.compiler.PatternCompiler`). This
class compiles/converts the flow objects and contained atoms into a
`networkx`_ directed graph (and tree structure) that contains the equivalent
atoms defined in the flow and any nested flows & atoms as well as the
constraints that are created by the application of the different flow
patterns. This graph (and tree) are what will be analyzed & traversed during
the engines execution. At this point a few helper object are also created and
saved to internal engine variables (these object help in execution of
atoms, analyzing the graph and performing other internal engine
activities). At the finishing of this stage a
:py:class:`~taskflow.engines.action_engine.runtime.Runtime` object is created
which contains references to all needed runtime components and its
:py:func:`~taskflow.engines.action_engine.runtime.Runtime.compile` is called
to compile a cache of frequently used execution helper objects.

Preparation
-----------

This stage (see :py:func:`~taskflow.engines.base.Engine.prepare`) starts by
setting up the storage needed for all atoms in the compiled graph, ensuring
that corresponding :py:class:`~taskflow.persistence.models.AtomDetail` (or
subclass of) objects are created for each node in the graph.

Validation
----------

This stage (see :py:func:`~taskflow.engines.base.Engine.validate`) performs
any final validation of the compiled (and now storage prepared) engine. It
compares the requirements that are needed to start execution and
what is currently provided or will be produced in the future. If there are
*any* atom requirements that are not satisfied (no known current provider or
future producer is found) then execution will **not** be allowed to continue.

Execution
---------

The graph (and helper objects) previously created are now used for guiding
further execution (see :py:func:`~taskflow.engines.base.Engine.run`). The
flow is put into the ``RUNNING`` :doc:`state <states>` and a
:py:class:`~taskflow.engines.action_engine.builder.MachineBuilder` state
machine object and runner object are built (using the `automaton`_ library).
That machine and associated runner then starts to take over and begins going
through the stages listed below (for a more visual diagram/representation see
the :ref:`engine state diagram <engine states>`).

.. note::

   The engine will respect the constraints imposed by the flow. For example,
   if an engine is executing a :py:class:`~taskflow.patterns.linear_flow.Flow`
   then it is constrained by the dependency graph which is linear in this
   case, and hence using a parallel engine may not yield any benefits if one
   is looking for concurrency.

Resumption
^^^^^^^^^^

One of the first stages is to analyze the :doc:`state <states>` of the tasks in
the graph, determining which ones have failed, which one were previously
running and determining what the intention of that task should now be
(typically an intention can be that it should ``REVERT``, or that it should
``EXECUTE`` or that it should be ``IGNORED``). This intention is determined by
analyzing the current state of the task; which is determined by looking at the
state in the task detail object for that task and analyzing edges of the graph
for things like retry atom which can influence what a tasks intention should be
(this is aided by the usage of the
:py:class:`~taskflow.engines.action_engine.analyzer.Analyzer` helper
object which was designed to provide helper methods for this analysis). Once
these intentions are determined and associated with each task (the intention is
also stored in the :py:class:`~taskflow.persistence.models.AtomDetail` object)
the :ref:`scheduling <scheduling>` stage starts.

.. _scheduling:

Scheduling
^^^^^^^^^^

This stage selects which atoms are eligible to run by using a
:py:class:`~taskflow.engines.action_engine.scheduler.Scheduler` implementation
(the default implementation looks at their intention, checking if predecessor
atoms have ran and so-on, using a
:py:class:`~taskflow.engines.action_engine.analyzer.Analyzer` helper
object as needed) and submits those atoms to a previously provided compatible
`executor`_ for asynchronous execution. This
:py:class:`~taskflow.engines.action_engine.scheduler.Scheduler` will return a
`future`_ object for each atom scheduled; all of which are collected into a
list of not done futures. This will end the initial round of scheduling and at
this point the engine enters the :ref:`waiting <waiting>` stage.

.. _waiting:

Waiting
^^^^^^^

In this stage the engine waits for any of the future objects previously
submitted to complete. Once one of the future objects completes (or fails) that
atoms result will be examined and finalized using a
:py:class:`~taskflow.engines.action_engine.completer.Completer` implementation.
It typically will persist results to a provided persistence backend (saved
into the corresponding :py:class:`~taskflow.persistence.models.AtomDetail`
and :py:class:`~taskflow.persistence.models.FlowDetail` objects via the
:py:class:`~taskflow.storage.Storage` helper) and reflect
the new state of the atom. At this point what typically happens falls into two
categories, one for if that atom failed and one for if it did not. If the atom
failed it may be set to a new intention such as ``RETRY`` or
``REVERT`` (other atoms that were predecessors of this failing atom may also
have there intention altered). Once this intention adjustment has happened a
new round of :ref:`scheduling <scheduling>` occurs and this process repeats
until the engine succeeds or fails (if the process running the engine dies the
above stages will be restarted and resuming will occur).

.. note::

    If the engine is suspended while the engine is going through the above
    stages this will stop any further scheduling stages from occurring and
    all currently executing work will be allowed to finish (see
    :ref:`suspension <suspension>`).

Finishing
---------

At this point the machine (and runner) that was built using the
:py:class:`~taskflow.engines.action_engine.builder.MachineBuilder` class has
now finished successfully, failed, or the execution was suspended. Depending on
which one of these occurs will cause the flow to enter a new state (typically
one of ``FAILURE``, ``SUSPENDED``, ``SUCCESS`` or ``REVERTED``).
:doc:`Notifications <notifications>` will be sent out about this final state
change (other state changes also send out notifications) and any failures that
occurred will be reraised (the failure objects are wrapped exceptions). If no
failures have occurred then the engine will have finished and if so desired the
:doc:`persistence <persistence>` can be used to cleanup any details that were
saved for this execution.

Special cases
=============

.. _suspension:

Suspension
----------

Each engine implements a :py:func:`~taskflow.engines.base.Engine.suspend`
method that can be used to *externally* (or in the future *internally*) request
that the engine stop :ref:`scheduling <scheduling>` new work. By default what
this performs is a transition of the flow state from ``RUNNING`` into a
``SUSPENDING`` state (which will later transition into a ``SUSPENDED`` state).
Since an engine may be remotely executing atoms (or locally executing them)
and there is currently no preemption what occurs is that the engines
:py:class:`~taskflow.engines.action_engine.builder.MachineBuilder` state
machine will detect this transition into ``SUSPENDING`` has occurred and the
state machine will avoid scheduling new work (it will though let active work
continue). After the current work has finished the engine will
transition from ``SUSPENDING`` into ``SUSPENDED`` and return from its
:py:func:`~taskflow.engines.base.Engine.run` method.


.. note::

    When :py:func:`~taskflow.engines.base.Engine.run`  is returned from at that
    point there *may* (but does not have to be, depending on what was active
    when :py:func:`~taskflow.engines.base.Engine.suspend` was called) be
    unfinished work in the flow that was not finished (but which can be
    resumed at a later point in time).

Scoping
=======

During creation of flows it is also important to understand the lookup
strategy (also typically known as `scope`_ resolution) that the engine you
are using will internally use. For example when a task ``A`` provides
result 'a' and a task ``B`` after ``A`` provides a different result 'a' and a
task ``C`` after ``A`` and after ``B`` requires 'a' to run, which one will
be selected?

Default strategy
----------------

When an engine is executing it internally interacts with the
:py:class:`~taskflow.storage.Storage` class
and that class interacts with the a
:py:class:`~taskflow.engines.action_engine.scopes.ScopeWalker` instance
and the :py:class:`~taskflow.storage.Storage` class uses the following
lookup order to find (or fail) a atoms requirement lookup/request:

#. Transient injected atom specific arguments.
#. Non-transient injected atom specific arguments.
#. Transient injected arguments (flow specific).
#. Non-transient injected arguments (flow specific).
#. First scope visited provider that produces the named result; note that
   if multiple providers are found in the same scope the *first* (the scope
   walkers yielded ordering defines what *first* means) that produced that
   result *and* can be extracted without raising an error is selected as the
   provider of the requested requirement.
#. Fails with :py:class:`~taskflow.exceptions.NotFound` if unresolved at this
   point (the ``cause`` attribute of this exception may have more details on
   why the lookup failed).

.. note::

    To examine this this information when debugging it is recommended to
    enable the ``BLATHER`` logging level (level 5). At this level the storage
    and scope code/layers will log what is being searched for and what is
    being found.

.. _scope: http://en.wikipedia.org/wiki/Scope_%28computer_science%29

Interfaces
==========

.. automodule:: taskflow.engines.base

Implementations
===============

.. automodule:: taskflow.engines.action_engine.engine

Components
----------

.. warning::

    External usage of internal engine functions, components and modules should
    be kept to a **minimum** as they may be altered, refactored or moved to
    other locations **without** notice (and without the typical deprecation
    cycle).

.. automodule:: taskflow.engines.action_engine.analyzer
.. automodule:: taskflow.engines.action_engine.builder
.. automodule:: taskflow.engines.action_engine.compiler
.. automodule:: taskflow.engines.action_engine.completer
.. automodule:: taskflow.engines.action_engine.deciders
.. automodule:: taskflow.engines.action_engine.executor
.. automodule:: taskflow.engines.action_engine.runtime
.. automodule:: taskflow.engines.action_engine.scheduler
.. autoclass:: taskflow.engines.action_engine.scopes.ScopeWalker
    :special-members: __iter__
.. automodule:: taskflow.engines.action_engine.traversal

Hierarchy
=========

.. inheritance-diagram::
    taskflow.engines.action_engine.engine.ActionEngine
    taskflow.engines.base.Engine
    taskflow.engines.worker_based.engine.WorkerBasedActionEngine
    :parts: 1

.. _automaton: http://docs.openstack.org/developer/automaton/
.. _multiprocessing: https://docs.python.org/2/library/multiprocessing.html
.. _future: https://docs.python.org/dev/library/concurrent.futures.html#future-objects
.. _executor: https://docs.python.org/dev/library/concurrent.futures.html#concurrent.futures.Executor
.. _networkx: https://networkx.github.io/
.. _futures backport: https://pypi.python.org/pypi/futures
.. _process pool executor: https://docs.python.org/dev/library/concurrent.futures.html#processpoolexecutor
