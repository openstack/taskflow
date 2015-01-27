-----
Types
-----

.. note::

    Even though these types **are** made for public consumption and usage
    should be encouraged/easily possible it should be noted that these may be
    moved out to new libraries at various points in the future (for example
    the ``FSM`` code *may* move to its own oslo supported ``automaton`` library
    at some point in the future [#f1]_). If you are using these
    types **without** using the rest of this library it is **strongly**
    encouraged that you be a vocal proponent of getting these made
    into *isolated* libraries (as using these types in this manner is not
    the expected and/or desired usage).

Cache
=====

.. automodule:: taskflow.types.cache

Failure
=======

.. automodule:: taskflow.types.failure

FSM
===

.. automodule:: taskflow.types.fsm

Futures
=======

.. automodule:: taskflow.types.futures

Graph
=====

.. automodule:: taskflow.types.graph

Notifier
========

.. automodule:: taskflow.types.notifier

Periodic
========

.. automodule:: taskflow.types.periodic

Table
=====

.. automodule:: taskflow.types.table

Timing
======

.. automodule:: taskflow.types.timing

Tree
====

.. automodule:: taskflow.types.tree

.. [#f1] See: https://review.openstack.org/#/c/141961 for a proposal to
         do this.
