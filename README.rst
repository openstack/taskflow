TaskFlow
========

.. image:: https://img.shields.io/pypi/v/taskflow.svg
    :target: https://pypi.python.org/pypi/taskflow/
    :alt: Latest Version

.. image:: https://img.shields.io/pypi/dm/taskflow.svg
    :target: https://pypi.python.org/pypi/taskflow/
    :alt: Downloads

A library to do [jobs, tasks, flows] in a highly available, easy to understand
and declarative manner (and more!) to be used with OpenStack and other
projects.

* Free software: Apache license
* Documentation: http://docs.openstack.org/developer/taskflow
* Source: http://git.openstack.org/cgit/openstack/taskflow
* Bugs: http://bugs.launchpad.net/taskflow/

Join us
-------

- http://launchpad.net/taskflow

Testing and requirements
------------------------

Requirements
~~~~~~~~~~~~

Because this project has many optional (pluggable) parts like persistence
backends and engines, we decided to split our requirements into two
parts: - things that are absolutely required (you can't use the project
without them) are put into ``requirements.txt``. The requirements
that are required by some optional part of this project (you can use the
project without them) are put into our ``test-requirements.txt`` file (so
that we can still test the optional functionality works as expected). If
you want to use the feature in question (`eventlet`_ or the worker based engine
that uses `kombu`_ or the `sqlalchemy`_ persistence backend or jobboards which
have an implementation built using `kazoo`_ ...), you should add
that requirement(s) to your project or environment.

Tox.ini
~~~~~~~

Our ``tox.ini`` file describes several test environments that allow to test
TaskFlow with different python versions and sets of requirements installed.
Please refer to the `tox`_ documentation to understand how to make these test
environments work for you.

Developer documentation
-----------------------

We also have sphinx documentation in ``docs/source``.

*To build it, run:*

::

    $ python setup.py build_sphinx

.. _kazoo: http://kazoo.readthedocs.org/
.. _sqlalchemy: http://www.sqlalchemy.org/
.. _kombu: http://kombu.readthedocs.org/
.. _eventlet: http://eventlet.net/
.. _tox: http://tox.testrun.org/
.. _developer documentation: http://docs.openstack.org/developer/taskflow/
