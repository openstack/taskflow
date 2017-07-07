================
 Using TaskFlow
================

Considerations
==============

Things to consider before (and during) development and integration with
TaskFlow into your project:

* Read over the `paradigm shifts`_ and engage the team in `IRC`_ (or via the
  `openstack-dev`_ mailing list) if these need more explanation (prefix
  ``[Oslo][TaskFlow]`` to your emails subject to get an even faster
  response).
* Follow (or at least attempt to follow) some of the established
  `best practices`_ (feel free to add your own suggested best practices).
* Keep in touch with the team (see above); we are all friendly and enjoy
  knowing your use cases and learning how we can help make your lives easier
  by adding or adjusting functionality in this library.

.. _IRC: irc://chat.freenode.net/openstack-state-management
.. _best practices: http://wiki.openstack.org/wiki/TaskFlow/Best_practices
.. _paradigm shifts: http://wiki.openstack.org/wiki/TaskFlow/Paradigm_shifts
.. _openstack-dev: mailto:openstack-dev@lists.openstack.org

User Guide
==========

.. toctree::
   :maxdepth: 2

   atoms
   arguments_and_results
   inputs_and_outputs

   patterns
   engines
   workers
   notifications
   persistence
   resumption

   jobs
   conductors

   examples

Miscellaneous
=============

.. toctree::
   :maxdepth: 2

   exceptions
   states
   types
   utils

Bookshelf
=========

A useful collection of links, documents, papers, similar
projects, frameworks and libraries.

.. note::

     Please feel free to submit your own additions and/or changes.

.. toctree::
   :maxdepth: 1

   shelf

Release notes
=============

.. toctree::
   :maxdepth: 2

   history
