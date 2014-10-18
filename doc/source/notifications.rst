===========================
Notifications and listeners
===========================

.. testsetup::

    from taskflow import task
    from taskflow.patterns import linear_flow
    from taskflow import engines

--------
Overview
--------

Engines provide a way to receive notification on task and flow state
transitions, which is useful for monitoring, logging, metrics, debugging
and plenty of other tasks.

To receive these notifications you should register a callback with
an instance of the :py:class:`~taskflow.utils.misc.Notifier`
class that is attached
to :py:class:`Engine <taskflow.engines.base.EngineBase>`
attributes ``task_notifier`` and ``notifier``.

TaskFlow also comes with a set of predefined :ref:`listeners <listeners>`, and
provides means to write your own listeners, which can be more convenient than
using raw callbacks.

--------------------------------------
Receiving notifications with callbacks
--------------------------------------

Flow notifications
------------------

To receive notification on flow state changes use the
:py:class:`~taskflow.utils.misc.Notifier` instance available as the
``notifier`` property of an engine.

A basic example is:

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
   >>> flo = linear_flow.Flow("cat-dog").add(
   ...   CatTalk(), DogTalk(provides="dog"))
   >>> eng = engines.load(flo, store={'meow': 'meow', 'woof': 'woof'})
   >>> eng.notifier.register("*", flow_transition)
   >>> eng.run()
   Flow 'cat-dog' transition to state RUNNING
   meow
   woof
   Flow 'cat-dog' transition to state SUCCESS

Task notifications
------------------

To receive notification on task state changes use the
:py:class:`~taskflow.utils.misc.Notifier` instance available as the
``task_notifier`` property of an engine.

A basic example is:

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

.. _listeners:

---------
Listeners
---------

TaskFlow comes with a set of predefined listeners -- helper classes that can be
used to do various actions on flow and/or tasks transitions. You can also
create your own listeners easily, which may be more convenient than using raw
callbacks for some use cases.

For example, this is how you can use
:py:class:`~taskflow.listeners.printing.PrintingListener`:

.. doctest::

   >>> from taskflow.listeners import printing
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
   >>> flo = linear_flow.Flow("cat-dog").add(
   ...   CatTalk(), DogTalk(provides="dog"))
   >>> eng = engines.load(flo, store={'meow': 'meow', 'woof': 'woof'})
   >>> with printing.PrintingListener(eng):
   ...   eng.run()
   ...
   taskflow.engines.action_engine.engine.SerialActionEngine: ... has moved flow 'cat-dog' (...) into state 'RUNNING'
   taskflow.engines.action_engine.engine.SerialActionEngine: ... has moved task 'CatTalk' (...) into state 'RUNNING'
   meow
   taskflow.engines.action_engine.engine.SerialActionEngine: ... has moved task 'CatTalk' (...) into state 'SUCCESS' with result 'cat' (failure=False)
   taskflow.engines.action_engine.engine.SerialActionEngine: ... has moved task 'DogTalk' (...) into state 'RUNNING'
   woof
   taskflow.engines.action_engine.engine.SerialActionEngine: ... has moved task 'DogTalk' (...) into state 'SUCCESS' with result 'dog' (failure=False)
   taskflow.engines.action_engine.engine.SerialActionEngine: ... has moved flow 'cat-dog' (...) into state 'SUCCESS'

Basic listener
--------------

.. autoclass:: taskflow.listeners.base.ListenerBase

Printing and logging listeners
------------------------------

.. autoclass:: taskflow.listeners.base.LoggingBase

.. autoclass:: taskflow.listeners.logging.LoggingListener

.. autoclass:: taskflow.listeners.logging.DynamicLoggingListener

.. autoclass:: taskflow.listeners.printing.PrintingListener

Timing listener
---------------

.. autoclass:: taskflow.listeners.timing.TimingListener

.. autoclass:: taskflow.listeners.timing.PrintingTimingListener
