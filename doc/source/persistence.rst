===========
Persistence
===========

Overview
========

In order to be able to receive inputs and create outputs from atoms (or other
engine processes) in a fault-tolerant way, there is a need to be able to place
what atoms output in some kind of location where it can be re-used by other
atoms (or used for other purposes). To accommodate this type of usage TaskFlow
provides an abstraction (provided by pluggable `stevedore`_ backends) that is
similar in concept to a running programs *memory*.

This abstraction serves the following *major* purposes:

* Tracking of what was done (introspection).
* Saving *memory* which allows for restarting from the last saved state
  which is a critical feature to restart and resume workflows (checkpointing).
* Associating additional metadata with atoms while running (without having
  those atoms need to save this data themselves). This makes it possible to
  add-on new metadata in the future without having to change the atoms
  themselves. For example the following can be saved:

  * Timing information (how long a task took to run).
  * User information (who the task ran as).
  * When a atom/workflow was ran (and why).

* Saving historical data (failures, successes, intermediary results...)
  to allow for retry atoms to be able to decide if they should should continue
  vs. stop.
* *Something you create...*

.. _stevedore: http://docs.openstack.org/developer/stevedore/

How it is used
==============

On :doc:`engine <engines>` construction typically a backend (it can be
optional) will be provided which satisfies the
:py:class:`~taskflow.persistence.base.Backend` abstraction. Along with
providing a backend object a
:py:class:`~taskflow.persistence.models.FlowDetail` object will also be
created and provided (this object will contain the details about the flow to be
ran) to the engine constructor (or associated :py:meth:`load()
<taskflow.engines.helpers.load>` helper functions).  Typically a
:py:class:`~taskflow.persistence.models.FlowDetail` object is created from a
:py:class:`~taskflow.persistence.models.LogBook` object (the book object acts
as a type of container for :py:class:`~taskflow.persistence.models.FlowDetail`
and :py:class:`~taskflow.persistence.models.AtomDetail` objects).

**Preparation**: Once an engine starts to run it will create a
:py:class:`~taskflow.storage.Storage` object which will act as the engines
interface to the underlying backend storage objects (it provides helper
functions that are commonly used by the engine, avoiding repeating code when
interacting with the provided
:py:class:`~taskflow.persistence.models.FlowDetail` and
:py:class:`~taskflow.persistence.base.Backend` objects). As an engine
initializes it will extract (or create)
:py:class:`~taskflow.persistence.models.AtomDetail` objects for each atom in
the workflow the engine will be executing.

**Execution:** When an engine beings to execute (see :doc:`engine <engines>`
for more of the details about how an engine goes about this process) it will
examine any previously existing
:py:class:`~taskflow.persistence.models.AtomDetail` objects to see if they can
be used for resuming; see :doc:`resumption <resumption>` for more details on
this subject. For atoms which have not finished (or did not finish correctly
from a previous run) they will begin executing only after any dependent inputs
are ready. This is done by analyzing the execution graph and looking at
predecessor :py:class:`~taskflow.persistence.models.AtomDetail` outputs and
states (which may have been persisted in a past run). This will result in
either using their previous information or by running those predecessors and
saving their output to the :py:class:`~taskflow.persistence.models.FlowDetail`
and :py:class:`~taskflow.persistence.base.Backend` objects. This
execution, analysis and interaction with the storage objects continues (what is
described here is a simplification of what really happens; which is quite a bit
more complex) until the engine has finished running (at which point the engine
will have succeeded or failed in its attempt to run the workflow).

**Post-execution:** Typically when an engine is done running the logbook would
be discarded (to avoid creating a stockpile of useless data) and the backend
storage would be told to delete any contents for a given execution. For certain
use-cases though it may be advantageous to retain logbooks and their contents.

A few scenarios come to mind:

* Post runtime failure analysis and triage (saving what failed and why).
* Metrics (saving timing information associated with each atom and using it
  to perform offline performance analysis, which enables tuning tasks and/or
  isolating and fixing slow tasks).
* Data mining logbooks to find trends (in failures for example).
* Saving logbooks for further forensics analysis.
* Exporting logbooks to `hdfs`_ (or other no-sql storage) and running some type
  of map-reduce jobs on them.

.. _hdfs: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html

.. note::

    It should be emphasized that logbook is the authoritative, and, preferably,
    the **only** (see :doc:`inputs and outputs <inputs_and_outputs>`) source of
    run-time state information (breaking this principle makes it
    hard/impossible to restart or resume in any type of automated fashion).
    When an atom returns a result, it should be written directly to a logbook.
    When atom or flow state changes in any way,  logbook is first to know (see
    :doc:`notifications <notifications>` for how a user  may also get notified
    of those same state changes). The logbook and a backend and associated
    storage helper class are responsible to store the actual data.  These
    components used together specify the persistence mechanism (how data is
    saved and where -- memory, database, whatever...) and the persistence
    policy (when data is saved -- every time it changes or at some particular
    moments or simply never).

Usage
=====

To select which persistence backend to use you should use the :py:meth:`fetch()
<taskflow.persistence.backends.fetch>` function which uses entrypoints
(internally using `stevedore`_) to fetch and configure your backend. This makes
it simpler than accessing the backend data types directly and provides a common
function from which a backend can be fetched.

Using this function to fetch a backend might look like:

.. code-block:: python

    from taskflow.persistence import backends

    ...
    persistence = backends.fetch(conf={
        "connection': "mysql",
        "user": ...,
        "password": ...,
    })
    book = make_and_save_logbook(persistence)
    ...

As can be seen from above the ``conf`` parameter acts as a dictionary that
is used to fetch and configure your backend. The restrictions on it are
the following:

* a dictionary (or dictionary like type), holding backend type with key
  ``'connection'`` and possibly type-specific backend parameters as other
  keys.

Types
=====

Memory
------

**Connection**: ``'memory'``

Retains all data in local memory (not persisted to reliable storage). Useful
for scenarios where persistence is not required (and also in unit tests).

.. note::

    See :py:class:`~taskflow.persistence.backends.impl_memory.MemoryBackend`
    for implementation details.

Files
-----

**Connection**: ``'dir'`` or ``'file'``

Retains all data in a directory & file based structure on local disk. Will be
persisted **locally** in the case of system failure (allowing for resumption
from the same local machine only). Useful for cases where a *more* reliable
persistence is desired along with the simplicity of files and directories (a
concept everyone is familiar with).

.. note::

    See :py:class:`~taskflow.persistence.backends.impl_dir.DirBackend`
    for implementation details.

SQLAlchemy
----------

**Connection**: ``'mysql'`` or ``'postgres'`` or ``'sqlite'``

Retains all data in a `ACID`_ compliant database using the `sqlalchemy`_
library for schemas, connections, and database interaction functionality.
Useful when you need a higher level of durability than offered by the previous
solutions. When using these connection types it is possible to resume a engine
from a peer machine (this does not apply when using sqlite).

Schema
^^^^^^

*Logbooks*

==========  ========  =============
Name        Type      Primary Key
==========  ========  =============
created_at  DATETIME  False
updated_at  DATETIME  False
uuid        VARCHAR   True
name        VARCHAR   False
meta        TEXT      False
==========  ========  =============

*Flow details*

===========  ========  =============
Name         Type      Primary Key
===========  ========  =============
created_at   DATETIME  False
updated_at   DATETIME  False
uuid         VARCHAR   True
name         VARCHAR   False
meta         TEXT      False
state        VARCHAR   False
parent_uuid  VARCHAR   False
===========  ========  =============

*Atom details*

===========  ========  =============
Name         Type      Primary Key
===========  ========  =============
created_at   DATETIME  False
updated_at   DATETIME  False
uuid         VARCHAR   True
name         VARCHAR   False
meta         TEXT      False
atom_type    VARCHAR   False
state        VARCHAR   False
intention    VARCHAR   False
results      TEXT      False
failure      TEXT      False
version      TEXT      False
parent_uuid  VARCHAR   False
===========  ========  =============

.. _sqlalchemy: http://www.sqlalchemy.org/docs/
.. _ACID: https://en.wikipedia.org/wiki/ACID

.. note::

    See :py:class:`~taskflow.persistence.backends.impl_sqlalchemy.SQLAlchemyBackend`
    for implementation details.

.. warning::

    Currently there is a size limit (not applicable for ``sqlite``) that the
    ``results`` will contain. This size limit will restrict how many prior
    failures a retry atom can contain. More information and a future fix
    will be posted to bug `1416088`_ (for the meantime try to ensure that
    your retry units history does not grow beyond ~80 prior results). This
    truncation can also be avoided by providing ``mysql_sql_mode`` as
    ``traditional`` when selecting your mysql + sqlalchemy based
    backend (see the `mysql modes`_ documentation for what this implies).

.. _1416088: http://bugs.launchpad.net/taskflow/+bug/1416088
.. _mysql modes: http://dev.mysql.com/doc/refman/5.0/en/sql-mode.html

Zookeeper
---------

**Connection**: ``'zookeeper'``

Retains all data in a `zookeeper`_ backend (zookeeper exposes operations on
files and directories, similar to the above ``'dir'`` or ``'file'`` connection
types). Internally the `kazoo`_ library is used to interact with zookeeper
to perform reliable, distributed and atomic operations on the contents of a
logbook represented as znodes. Since zookeeper is also distributed it is also
able to resume a engine from a peer machine (having similar functionality
as the database connection types listed previously).

.. note::

    See :py:class:`~taskflow.persistence.backends.impl_zookeeper.ZkBackend`
    for implementation details.

.. _zookeeper: http://zookeeper.apache.org
.. _kazoo: http://kazoo.readthedocs.org/

Interfaces
==========

.. automodule:: taskflow.persistence.backends
.. automodule:: taskflow.persistence.base
.. automodule:: taskflow.persistence.path_based

Models
======

.. automodule:: taskflow.persistence.models

Implementations
===============

Memory
------

.. automodule:: taskflow.persistence.backends.impl_memory

Files
-----

.. automodule:: taskflow.persistence.backends.impl_dir

SQLAlchemy
----------

.. automodule:: taskflow.persistence.backends.impl_sqlalchemy

Zookeeper
---------

.. automodule:: taskflow.persistence.backends.impl_zookeeper

Storage
=======

.. automodule:: taskflow.storage

Hierarchy
=========

.. inheritance-diagram::
    taskflow.persistence.base
    taskflow.persistence.backends.impl_dir
    taskflow.persistence.backends.impl_memory
    taskflow.persistence.backends.impl_sqlalchemy
    taskflow.persistence.backends.impl_zookeeper
    :parts: 2
