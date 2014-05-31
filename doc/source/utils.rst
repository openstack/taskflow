-----
Utils
-----

There are various helper utils that are part of TaskFlows internal usage (and
external/public usage of these helpers should be kept to a minimum as these
utility functions may be altered more often in the future).

External usage
==============

The following classes and modules are *recommended* for external usage:

.. autoclass:: taskflow.utils.misc.Failure
    :members:

.. autoclass:: taskflow.utils.eventlet_utils.GreenExecutor
    :members:

.. autofunction:: taskflow.utils.persistence_utils.temporary_log_book

.. autofunction:: taskflow.utils.persistence_utils.temporary_flow_detail

.. autofunction:: taskflow.utils.persistence_utils.pformat
