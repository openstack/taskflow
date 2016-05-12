=====================
Arguments and results
=====================

.. |task.execute| replace:: :py:meth:`~taskflow.atom.Atom.execute`
.. |task.revert| replace:: :py:meth:`~taskflow.atom.Atom.revert`
.. |retry.execute| replace:: :py:meth:`~taskflow.retry.Retry.execute`
.. |retry.revert| replace:: :py:meth:`~taskflow.retry.Retry.revert`
.. |Retry| replace:: :py:class:`~taskflow.retry.Retry`
.. |Task| replace:: :py:class:`Task <taskflow.task.Task>`

In TaskFlow, all flow and task state goes to (potentially persistent) storage
(see :doc:`persistence <persistence>` for more details). That includes all the
information that :doc:`atoms <atoms>` (e.g. tasks, retry objects...) in the
workflow need when they are executed, and all the information task/retry
produces (via serializable results). A developer who implements tasks/retries
or flows can specify what arguments a task/retry accepts and what result it
returns in several ways. This document will help you understand what those ways
are and how to use those ways to accomplish your desired usage pattern.

.. glossary::

    Task/retry arguments
        Set of names of task/retry arguments available as the ``requires``
        and/or ``optional`` property of the task/retry instance. When a task or
        retry object is about to be executed values with these names are
        retrieved from storage and passed to the ``execute`` method of the
        task/retry.  If any names in the ``requires`` property cannot be
        found in storage, an exception will be thrown.  Any names in the
        ``optional`` property that cannot be found are ignored.

    Task/retry results
        Set of names of task/retry results (what task/retry provides) available
        as ``provides`` property of task or retry instance. After a task/retry
        finishes successfully, its result(s) (what the ``execute`` method
        returns) are available by these names from storage (see examples
        below).


.. testsetup::

    from taskflow import task


Arguments specification
=======================

There are different ways to specify the task argument ``requires`` set.

Arguments inference
-------------------

Task/retry arguments can be inferred from arguments of the |task.execute|
method of a task (or the |retry.execute| of a retry object).

.. doctest::

    >>> class MyTask(task.Task):
    ...     def execute(self, spam, eggs, bacon=None):
    ...         return spam + eggs
    ...
    >>> sorted(MyTask().requires)
    ['eggs', 'spam']
    >>> sorted(MyTask().optional)
    ['bacon']

Inference from the method signature is the ''simplest'' way to specify
arguments. Special arguments like ``self``, ``*args`` and ``**kwargs`` are
ignored during inference (as these names have special meaning/usage in python).

.. doctest::

    >>> class UniTask(task.Task):
    ...     def execute(self, *args, **kwargs):
    ...         pass
    ...
    >>> sorted(UniTask().requires)
    []

.. make vim sphinx highlighter* happy**


Rebinding
---------

**Why:** There are cases when the value you want to pass to a task/retry is
stored with a name other than the corresponding arguments name. That's when the
``rebind`` constructor parameter comes in handy. Using it the flow author
can instruct the engine to fetch a value from storage by one name, but pass it
to a tasks/retries ``execute`` method with another name. There are two possible
ways of accomplishing this.

The first is to pass a dictionary that maps the argument name to the name
of a saved value.

For example, if you have task::

    class SpawnVMTask(task.Task):

        def execute(self, vm_name, vm_image_id, **kwargs):
            pass  # TODO(imelnikov): use parameters to spawn vm

and you saved ``'vm_name'`` with ``'name'`` key in storage, you can spawn a vm
with such ``'name'`` like this::

    SpawnVMTask(rebind={'vm_name': 'name'})

The second way is to pass a tuple/list/dict of argument names. The length of
the tuple/list/dict should not be less then number of required parameters.

For example, you can achieve the same effect as the previous example with::

    SpawnVMTask(rebind_args=('name', 'vm_image_id'))

This is equivalent to a more elaborate::

    SpawnVMTask(rebind=dict(vm_name='name',
                            vm_image_id='vm_image_id'))

In both cases, if your task (or retry) accepts arbitrary arguments
with the ``**kwargs`` construct, you can specify extra arguments.

::

    SpawnVMTask(rebind=('name', 'vm_image_id', 'admin_key_name'))

When such task is about to be executed, ``name``, ``vm_image_id`` and
``admin_key_name`` values are fetched from storage and value from ``name`` is
passed to |task.execute| method as ``vm_name``, value from ``vm_image_id`` is
passed as ``vm_image_id``, and value from ``admin_key_name`` is passed as
``admin_key_name`` parameter in ``kwargs``.

Manually specifying requirements
--------------------------------

**Why:** It is often useful to manually specify the requirements of a task,
either by a task author or by the flow author (allowing the flow author to
override the task requirements).

To accomplish this when creating your task use the constructor to specify
manual requirements.  Those manual requirements (if they are not functional
arguments) will appear in the ``kwargs`` of the |task.execute| method.

.. doctest::

    >>> class Cat(task.Task):
    ...     def __init__(self, **kwargs):
    ...         if 'requires' not in kwargs:
    ...             kwargs['requires'] = ("food", "milk")
    ...         super(Cat, self).__init__(**kwargs)
    ...     def execute(self, food, **kwargs):
    ...         pass
    ...
    >>> cat = Cat()
    >>> sorted(cat.requires)
    ['food', 'milk']

.. make vim sphinx highlighter happy**

When constructing a task instance the flow author can also add more
requirements if desired.  Those manual requirements (if they are not functional
arguments) will appear in the ``kwargs`` parameter of the |task.execute|
method.

.. doctest::

    >>> class Dog(task.Task):
    ...     def execute(self, food, **kwargs):
    ...         pass
    >>> dog = Dog(requires=("water", "grass"))
    >>> sorted(dog.requires)
    ['food', 'grass', 'water']

.. make vim sphinx highlighter happy**

If the flow author desires she can turn the argument inference off and override
requirements manually. Use this at your own **risk** as you must be careful to
avoid invalid argument mappings.

.. doctest::

    >>> class Bird(task.Task):
    ...     def execute(self, food, **kwargs):
    ...         pass
    >>> bird = Bird(requires=("food", "water", "grass"), auto_extract=False)
    >>> sorted(bird.requires)
    ['food', 'grass', 'water']

.. make vim sphinx highlighter happy**

Results specification
=====================

In python, function results are not named, so we can not infer what a
task/retry returns. This is important since the complete result (what the
task |task.execute| or retry |retry.execute| method returns) is saved
in (potentially persistent) storage, and it is typically (but not always)
desirable to make those results accessible to others. To accomplish this
the task/retry specifies names of those values via its ``provides`` constructor
parameter or by its default provides attribute.

Examples
--------

Returning one value
+++++++++++++++++++

If task returns just one value, ``provides`` should be string -- the
name of the value.

.. doctest::

    >>> class TheAnswerReturningTask(task.Task):
    ...    def execute(self):
    ...        return 42
    ...
    >>> sorted(TheAnswerReturningTask(provides='the_answer').provides)
    ['the_answer']

Returning a tuple
+++++++++++++++++

For a task that returns several values, one option (as usual in python) is to
return those values via a ``tuple``.

::

    class BitsAndPiecesTask(task.Task):
        def execute(self):
            return 'BITs', 'PIECEs'

Then, you can give the value individual names, by passing a tuple or list as
``provides`` parameter:

::

    BitsAndPiecesTask(provides=('bits', 'pieces'))

After such task is executed, you (and the engine, which is useful for other
tasks) will be able to get those elements from storage by name:

::

    >>> storage.fetch('bits')
    'BITs'
    >>> storage.fetch('pieces')
    'PIECEs'

Provides argument can be shorter then the actual tuple returned by a task --
then extra values are ignored (but, as expected, **all** those values are saved
and passed to the task |task.revert| or retry |retry.revert| method).

.. note::

    Provides arguments tuple can also be longer then the actual tuple returned
    by task -- when this happens the extra parameters are left undefined: a
    warning is printed to logs and if use of such parameter is attempted a
    :py:class:`~taskflow.exceptions.NotFound`  exception is raised.

Returning a dictionary
++++++++++++++++++++++

Another option is to return several values as a dictionary (aka a ``dict``).

::

    class BitsAndPiecesTask(task.Task):

        def execute(self):
            return {
                'bits': 'BITs',
                'pieces': 'PIECEs'
            }

TaskFlow expects that a dict will be returned if ``provides`` argument is a
``set``:

::

    BitsAndPiecesTask(provides=set(['bits', 'pieces']))

After such task executes, you (and the engine, which is useful for other tasks)
will be able to get elements from storage by name:

::

    >>> storage.fetch('bits')
    'BITs'
    >>> storage.fetch('pieces')
    'PIECEs'

.. note::

    If some items from the dict returned by the task are not present in the
    provides arguments -- then extra values are ignored (but, of course, saved
    and passed to the |task.revert| method). If the provides argument has some
    items not present in the actual dict returned by the task -- then extra
    parameters are left undefined: a warning is printed to logs and if use of
    such parameter is attempted a :py:class:`~taskflow.exceptions.NotFound`
    exception is raised.

Default provides
++++++++++++++++

As mentioned above, the default base class provides nothing, which means
results are not accessible to other tasks/retries in the flow.

The author can override this and specify default value for provides using
the ``default_provides`` class/instance variable:

::

    class BitsAndPiecesTask(task.Task):
        default_provides = ('bits', 'pieces')
        def execute(self):
            return 'BITs', 'PIECEs'

Of course, the flow author can override this to change names if needed:

::

    BitsAndPiecesTask(provides=('b', 'p'))

or to change structure -- e.g. this instance will make tuple accessible
to other tasks by name ``'bnp'``:

::

    BitsAndPiecesTask(provides='bnp')

or the flow author may want to return default behavior and hide the results of
the task from other tasks in the flow (e.g. to avoid naming conflicts):

::

    BitsAndPiecesTask(provides=())

Revert arguments
================

To revert a task the :doc:`engine <engines>` calls the tasks
|task.revert| method. This method should accept the same arguments
as the |task.execute| method of the task and one more special keyword
argument, named ``result``.

For ``result`` value, two cases are possible:

* If the task is being reverted because it failed (an exception was raised
  from its |task.execute| method), the ``result`` value is an instance of a
  :py:class:`~taskflow.types.failure.Failure` object that holds the exception
  information.

* If the task is being reverted because some other task failed, and this task
  finished successfully, ``result`` value is the result fetched from storage:
  ie, what the |task.execute| method returned.

All other arguments are fetched from storage in the same way it is done for
|task.execute| method.

To determine if a task failed you can check whether ``result`` is instance of
:py:class:`~taskflow.types.failure.Failure`::

    from taskflow.types import failure

    class RevertingTask(task.Task):

        def execute(self, spam, eggs):
            return do_something(spam, eggs)

        def revert(self, result, spam, eggs):
            if isinstance(result, failure.Failure):
                print("This task failed, exception: %s"
                      % result.exception_str)
            else:
                print("do_something returned %r" % result)

If this task failed (ie ``do_something`` raised an exception) it will print
``"This task failed, exception:"`` and a exception message on revert. If this
task finished successfully, it will print ``"do_something returned"`` and a
representation of the ``do_something`` result.

Retry arguments
===============

A |Retry| controller works with arguments in the same way as a |Task|. But it
has an additional parameter ``'history'`` that is itself a
:py:class:`~taskflow.retry.History` object that contains what failed over all
the engines attempts (aka the outcomes). The history object can be
viewed as a tuple that contains a result of the previous retries run and a
table/dict where each key is a failed atoms name and each value is
a :py:class:`~taskflow.types.failure.Failure` object.

Consider the following implementation::

  class MyRetry(retry.Retry):

      default_provides = 'value'

      def on_failure(self, history, *args, **kwargs):
          print(list(history))
          return RETRY

      def execute(self, history, *args, **kwargs):
          print(list(history))
          return 5

      def revert(self, history, *args, **kwargs):
          print(list(history))

Imagine the above retry had returned a value ``'5'`` and then some task ``'A'``
failed with some exception.  In this case ``on_failure`` method will receive
the following history (printed as a list)::

    [('5', {'A': failure.Failure()})]

At this point (since the implementation returned ``RETRY``) the
|retry.execute| method will be called again and it will receive the same
history and it can then return a value that subsequent tasks can use to alter
their behavior.

If instead the |retry.execute| method itself raises an exception,
the |retry.revert| method of the implementation will be called and
a :py:class:`~taskflow.types.failure.Failure` object will be present in the
history object instead of the typical result.

.. note::

    After a |Retry| has been reverted, the objects history will be cleaned.
