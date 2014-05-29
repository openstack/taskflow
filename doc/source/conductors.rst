----------
Conductors
----------

Overview
========

Conductors in TaskFlow provide a mechanism that unifies the various TaskFlow
concepts under a single easy to use (as plug-and-play as we can make it)
construct.

They are responsible for the following:

* Interacting with :doc:`jobboards <jobs>` (examining and claiming
  :doc:`jobs <jobs>`).
* Creating :doc:`engines <engines>` from the claimed jobs (using
  :ref:`factories <resumption factories>` to reconstruct the contained
  tasks and flows to be executed).
* Dispatching the engine using the provided :doc:`persistence <persistence>`
  layer and engine configuration.
* Completing or abandoning the claimed job (depending on dispatching and
  execution outcome).
* *Rinse and repeat*.

.. note::

     They are inspired by and have similar responsiblities
     as `railroad conductors`_.

Interfaces
==========

.. automodule:: taskflow.conductors.base
.. automodule:: taskflow.conductors.single_threaded

Hierarchy
=========

.. inheritance-diagram::
    taskflow.conductors.base
    taskflow.conductors.single_threaded
    :parts: 1

.. _railroad conductors: http://en.wikipedia.org/wiki/Conductor_%28transportation%29
