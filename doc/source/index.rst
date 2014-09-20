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
   :maxdepth: 3

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
   types

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. [#f1] It should be noted that even though it is designed with OpenStack
         integration in mind, and that is where most of its *current*
         integration is it aims to be generally usable and useful in any
         project.
