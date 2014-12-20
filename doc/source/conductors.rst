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

     They are inspired by and have similar responsibilities
     as `railroad conductors`_.

Considerations
==============

Some usage considerations should be used when using a conductor to make sure
it's used in a safe and reliable manner. Eventually we hope to make these
non-issues but for now they are worth mentioning.

Endless cycling
---------------

**What:** Jobs that fail (due to some type of internal error) on one conductor
will be abandoned by that conductor and then another conductor may experience
those same errors and abandon it (and repeat). This will create a job
abandonment cycle that will continue for as long as the job exists in an
claimable state.

**Example:**

.. image:: img/conductor_cycle.png
   :scale: 70%
   :alt: Conductor cycling

**Alleviate by:**

#. Forcefully delete jobs that have been failing continuously after a given
   number of conductor attempts. This can be either done manually or
   automatically via scripts (or other associated monitoring).
#. Resolve the internal error's cause (storage backend failure, other...).
#. Help implement `jobboard garbage binning`_.

.. _jobboard garbage binning: https://blueprints.launchpad.net/taskflow/+spec/jobboard-garbage-bin

Interfaces
==========

.. automodule:: taskflow.conductors.base

Implementations
===============

.. automodule:: taskflow.conductors.single_threaded

Hierarchy
=========

.. inheritance-diagram::
    taskflow.conductors.base
    taskflow.conductors.single_threaded
    :parts: 1

.. _railroad conductors: http://en.wikipedia.org/wiki/Conductor_%28transportation%29
