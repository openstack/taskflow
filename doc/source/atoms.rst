------------------------
Atoms, Tasks and Retries
------------------------

An atom is the smallest unit in taskflow which acts as the base for other
classes. Atoms have a name and a version (if applicable). Atom is expected
to name desired input values (requirements) and name outputs (provided
values), see :doc:`arguments_and_results` page for complete reference
about it.

.. automodule:: taskflow.atom

Task
=====

A task (derived from an atom) is the smallest possible unit of work that can
have an execute & rollback sequence associated with it.

.. automodule:: taskflow.task

Retry
=====

A retry (derived from an atom) is a special unit that handles flow errors,
controlls flow execution and can retry it with another parameters if needed.

.. automodule:: taskflow.retry

