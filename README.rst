========================
Team and repository tags
========================

.. image:: https://governance.openstack.org/tc/badges/taskflow.svg
    :target: https://governance.openstack.org/tc/reference/tags/index.html

.. Change things from this point on

TaskFlow
========

.. image:: https://img.shields.io/pypi/v/taskflow.svg
    :target: https://pypi.org/project/taskflow/
    :alt: Latest Version

A library to do [jobs, tasks, flows] in a highly available, easy to understand
and declarative manner (and more!) to be used with OpenStack and other
projects.

* Free software: Apache license
* Documentation: https://docs.openstack.org/taskflow/latest/
* Source: https://opendev.org/openstack/taskflow
* Bugs: https://bugs.launchpad.net/taskflow/
* Release notes: https://docs.openstack.org/releasenotes/taskflow/

Join us
-------

- https://launchpad.net/taskflow

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

.. _kazoo: https://kazoo.readthedocs.io/en/latest/
.. _sqlalchemy: https://www.sqlalchemy.org/
.. _kombu: https://kombu.readthedocs.io/en/latest/
.. _eventlet: http://eventlet.net/
.. _tox: https://tox.testrun.org/
