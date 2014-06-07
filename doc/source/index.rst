TaskFlow
========

*TaskFlow is a Python library for OpenStack that helps make task execution
easy, consistent, and reliable.*

.. note::

    Additional documentation is also hosted on wiki:
    https://wiki.openstack.org/wiki/TaskFlow

Contents
========

.. toctree::
   :maxdepth: 2

   atoms
   arguments_and_results
   inputs_and_outputs

   patterns
   engines
   notifications
   persistence
   resumption

   jobs
   conductors

.. toctree::
   :hidden:

   workers

Considerations
--------------

Things to consider before (and during) development and integration with
TaskFlow into your project:

* Read over the `paradigm shifts`_ and engage the team in `IRC`_ (or via the
  `openstack-dev`_ mailing list) if these need more explanation (prefix
  ``[TaskFlow]`` to your emails subject to get an even faster response).
* Follow (or at least attempt to follow) some of the established
  `best practices`_ (feel free to add your own suggested best practices).

.. warning::

        External usage of internal helpers and other internal utility functions
        and modules should be kept to a *minimum* as these may be altered,
        refactored or moved *without* notice. If you are unsure whether to use
        a function, class, or module, please ask (see above).

.. _IRC: irc://chat.freenode.net/openstack-state-management
.. _best practices: http://wiki.openstack.org/wiki/TaskFlow/Best_practices
.. _paradigm shifts: http://wiki.openstack.org/wiki/TaskFlow/Paradigm_shifts
.. _openstack-dev: mailto:openstack-dev@lists.openstack.org

Miscellaneous
-------------

.. toctree::
   :maxdepth: 2

   exceptions
   states
   examples

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

