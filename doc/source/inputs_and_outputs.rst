==================
Inputs and Outputs
==================

--------
Overview
--------

In taskflow there are multiple ways to design how your tasks/flows and engines get inputs and produce outputs. This document will help you understand what those ways are and how to use those ways to accomplish your desired taskflow usage pattern as well as include examples that show common ways of providing input and getting output.

------------------------------
Task & Flow Inputs and Outputs
------------------------------

A task accept inputs via task arguments and provides outputs via task results (see :doc:`arguments_and_results` for more details). This the standard and recommended way to pass data from one task to another. Of course not every task argument needs to be provided to some other task of a flow, and not every task result should be consumed by every task.

If some value is required by one or more tasks of a flow, but is not provided by any task, it is considered to be flow input, and MUST be put into the storage before the flow is run. A set of names required by a flow can be retrieved via that flows requires property. These names can be used to determine what names may be applicable for placing in storage ahead of time and which names are not applicable.

All values provided by tasks of the flow are considered to be flow outputs; the set of names of such values is available via the provides property of the flow.

.. testsetup::

    from taskflow import task
    from taskflow.patterns import linear_flow
    from taskflow import engines
    from taskflow.listeners import printing

For example:

.. doctest::

   >>> class MyTask(task.Task):
   ...     def execute(self, **kwargs):
   ...         return 1, 2
   ...
   >>> flow = linear_flow.Flow('test').add(
   ...     MyTask(requires='a', provides=('b', 'c')),
   ...     MyTask(requires='b', provides='d')
   ... )
   >>> flow.requires
   set(['a'])
   >>> sorted(flow.provides)
   ['b', 'c', 'd']

As you can see, this flow does not require b, as it is provided by the fist task.

.. note::
   There is no difference between processing of Task and Retry inputs and outputs.

-------------------------
Engine Inputs and Outputs
-------------------------

Storage
=======

The storage layer is how an engine persists flow and task details.

For more in-depth design details: :doc:`persistence`.

Inputs
------

**The problem:** you should prepopulate storage with all required flow inputs before running it:

.. doctest::

   >>> class CatTalk(task.Task):
   ...   def execute(self, meow):
   ...     print meow
   ...     return "cat"
   ...
   >>> class DogTalk(task.Task):
   ...   def execute(self, woof):
   ...     print woof
   ...     return "dog"
   ...
   >>> flo = linear_flow.Flow("cat-dog")
   >>> flo.add(CatTalk(), DogTalk(provides="dog"))
   <taskflow.patterns.linear_flow.Flow object at 0x...>
   >>> engines.run(flo)
   Traceback (most recent call last):
      ...
   taskflow.exceptions.MissingDependencies: taskflow.patterns.linear_flow.Flow: cat-dog;
   2 requires ['meow', 'woof'] but no other entity produces said requirements

**The solution:** provide necessary data via store parameter of engines.run:

.. doctest::

   >>> class CatTalk(task.Task):
   ...   def execute(self, meow):
   ...     print meow
   ...     return "cat"
   ...
   >>> class DogTalk(task.Task):
   ...   def execute(self, woof):
   ...     print woof
   ...     return "dog"
   ...
   >>> flo = linear_flow.Flow("cat-dog")
   >>> flo.add(CatTalk(), DogTalk(provides="dog"))
   <taskflow.patterns.linear_flow.Flow object at 0x...>
   >>> engines.run(flo, store={'meow': 'meow', 'woof': 'woof'})
   meow
   woof
   {'meow': 'meow', 'woof': 'woof', 'dog': 'dog'}

.. note::
   You can also directly interact with the engine storage layer to add additional values although you must use the load method instead.

.. doctest::

   >>> flo = linear_flow.Flow("cat-dog")
   >>> flo.add(CatTalk(), DogTalk(provides="dog"))
   <taskflow.patterns.linear_flow.Flow object at 0x...>
   >>> eng = engines.load(flo, store={'meow': 'meow'})
   >>> eng.storage.inject({"woof": "bark"})
   >>> eng.run()
   meow
   bark

Outputs
-------

As you can see from examples above, run method returns all flow outputs in a dict. This same data can be fetched via fetch_all method of the storage, or in a more precise manner by using fetch method.

For example:

.. doctest::

   >>> eng = engines.load(flo, store={'meow': 'meow', 'woof': 'woof'})
   >>> eng.run()
   meow
   woof
   >>> print(eng.storage.fetch_all())
   {'meow': 'meow', 'woof': 'woof', 'dog': 'dog'}
   >>> print(eng.storage.fetch("dog"))
   dog

Notifications
=============

**What:** engines provide a way to receive notification on task and flow state transitions.

**Why:** state transition notifications are useful for monitoring, logging, metrics, debugging, affecting further engine state (and other unknown future usage).

Flow notifications
------------------

A basic example is the following:

.. doctest::

   >>> class CatTalk(task.Task):
   ...   def execute(self, meow):
   ...     print(meow)
   ...     return "cat"
   ...
   >>> class DogTalk(task.Task):
   ...   def execute(self, woof):
   ...     print(woof)
   ...     return 'dog'
   ...
   >>> def flow_transition(state, details):
   ...     print("Flow '%s' transition to state %s" % (details['flow_name'], state))
   ...
   >>>
   >>> flo = linear_flow.Flow("cat-dog")
   >>> flo.add(CatTalk(), DogTalk(provides="dog"))
   <taskflow.patterns.linear_flow.Flow object at 0x...>
   >>> eng = engines.load(flo, store={'meow': 'meow', 'woof': 'woof'})
   >>> eng.notifier.register("*", flow_transition)
   >>> eng.run()
   Flow 'cat-dog' transition to state RUNNING
   meow
   woof
   Flow 'cat-dog' transition to state SUCCESS

Task notifications
------------------

A basic example is the following:

.. doctest::

   >>> class CatTalk(task.Task):
   ...   def execute(self, meow):
   ...     print(meow)
   ...     return "cat"
   ...
   >>> class DogTalk(task.Task):
   ...   def execute(self, woof):
   ...     print(woof)
   ...     return 'dog'
   ...
   >>> def task_transition(state, details):
   ...     print("Task '%s' transition to state %s" % (details['task_name'], state))
   ...
   >>>
   >>> flo = linear_flow.Flow("cat-dog")
   >>> flo.add(CatTalk(), DogTalk(provides="dog"))
   <taskflow.patterns.linear_flow.Flow object at 0x...>
   >>> eng = engines.load(flo, store={'meow': 'meow', 'woof': 'woof'})
   >>> eng.task_notifier.register("*", task_transition)
   >>> eng.run()
   Task 'CatTalk' transition to state RUNNING
   meow
   Task 'CatTalk' transition to state SUCCESS
   Task 'DogTalk' transition to state RUNNING
   woof
   Task 'DogTalk' transition to state SUCCESS

Common notification classes
---------------------------

There exists common helper classes that can be used to accomplish common ways of notifying.

Helper to output to stderr/stdout
Helper to output to a logging backend

A basic example is the following:

.. doctest::

   >>> class CatTalk(task.Task):
   ...   def execute(self, meow):
   ...     print(meow)
   ...     return "cat"
   ...
   >>> class DogTalk(task.Task):
   ...   def execute(self, woof):
   ...     print(woof)
   ...     return 'dog'
   ...
   >>>
   >>> flo = linear_flow.Flow("cat-dog")
   >>> flo.add(CatTalk(), DogTalk(provides="dog"))
   <taskflow.patterns.linear_flow.Flow object at 0x...>
   >>> eng = engines.load(flo, store={'meow': 'meow', 'woof': 'woof'})
   >>> with printing.PrintingListener(eng):
   ...   eng.run()
   ...
   taskflow.engines.action_engine.engine.SingleThreadedActionEngine: ... has moved flow 'cat-dog' (...) into state 'RUNNING'
   taskflow.engines.action_engine.engine.SingleThreadedActionEngine: ... has moved task 'CatTalk' (...) into state 'RUNNING'
   meow
   taskflow.engines.action_engine.engine.SingleThreadedActionEngine: ... has moved task 'CatTalk' (...) into state 'SUCCESS' with result 'cat' (failure=False)
   taskflow.engines.action_engine.engine.SingleThreadedActionEngine: ... has moved task 'DogTalk' (...) into state 'RUNNING'
   woof
   taskflow.engines.action_engine.engine.SingleThreadedActionEngine: ... has moved task 'DogTalk' (...) into state 'SUCCESS' with result 'dog' (failure=False)
   taskflow.engines.action_engine.engine.SingleThreadedActionEngine: ... has moved flow 'cat-dog' (...) into state 'SUCCESS'


