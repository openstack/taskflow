TaskFlow
========

*TaskFlow is a Python library that helps to make task execution easy,
consistent and reliable.* [#f1]_

.. note::

    If you are just getting started or looking for an overview please
    visit: http://wiki.openstack.org/wiki/TaskFlow which provides better
    introductory material, description of high level goals and related content.

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

Supplementary
=============

Examples
--------

While developing TaskFlow the team has worked *hard* to make sure the various
concepts are explained by *relevant* examples. Here are a few selected examples
to get started (ordered by *perceived* complexity):

.. toctree::
   :maxdepth: 2

   examples

To explore more of these examples please check out the `examples`_ directory
in the TaskFlow `source tree`_.

.. note::

    If the examples provided are not satisfactory (or up to your
    standards) contributions are welcome and very much appreciated to help
    improve them. The higher the quality and the clearer the examples are the
    better and more useful they are for everyone.

.. _examples: http://git.openstack.org/cgit/openstack/taskflow/tree/taskflow/examples
.. _source tree: http://git.openstack.org/cgit/openstack/taskflow/

Considerations
--------------

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

Miscellaneous
-------------

.. toctree::
   :maxdepth: 2

   exceptions
   states
   types
   utils

Bookshelf
---------

A useful collection of links, documents, papers, similar
projects, frameworks and libraries.

.. note::

     Please feel free to submit your own additions and/or changes.

.. toctree::
   :maxdepth: 1

   shelf

Release notes
-------------

.. toctree::
   :maxdepth: 2

   history

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. [#f1] It should be noted that even though it is designed with OpenStack
         integration in mind, and that is where most of its *current*
         integration is it aims to be generally usable and useful in any
         project.
