------------------------
Atoms, Tasks and Retries
------------------------

An atom is the smallest unit in taskflow which acts as the base for other
classes. Atoms have a name and a version (if applicable). An atom is expected
to name desired input values (requirements) and name outputs (provided
values), see the :doc:`arguments and results <arguments_and_results>` page for
a complete reference about these inputs and outputs.

.. automodule:: taskflow.atom

Task
=====

A task (derived from an atom) is the smallest possible unit of work that can
have an execute & rollback sequence associated with it.

.. automodule:: taskflow.task

Retry
=====

A retry (derived from an atom) is a special unit that handles flow errors,
controls flow execution and can retry atoms with another parameters if needed.
It is useful to allow for alternate ways of retrying atoms when they fail so
the whole flow can proceed even when a group of atoms fail.

.. automodule:: taskflow.retry

