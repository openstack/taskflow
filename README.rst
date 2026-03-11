TaskFlow
========

.. image:: https://governance.openstack.org/tc/badges/taskflow.svg

.. Change things from this point on

.. image:: https://img.shields.io/pypi/v/taskflow.svg
    :target: https://pypi.org/project/taskflow/
    :alt: Latest Version

.. image:: https://img.shields.io/pypi/dm/taskflow.svg
    :target: https://pypi.org/project/taskflow/
    :alt: Downloads

A library to do [jobs, tasks, flows] in a highly available, easy to understand
and declarative manner (and more!) to be used with OpenStack and other
projects.

* Free software: Apache license
* Documentation: https://docs.openstack.org/taskflow/latest/
* Source: https://opendev.org/openstack/taskflow
* Bugs: https://bugs.launchpad.net/taskflow/
* Release notes: https://docs.openstack.org/releasenotes/taskflow/

Installation
------------

The library can be installed from PyPI:

.. code-block:: shell

    pip install taskflow

Because this project has many optional (pluggable) parts like persistence
backends and engines, we decided to split our requirements into two
parts: - things that are absolutely required (you can't use the project without
them) and things that are required by some optional part of this project (you
can use the project without them). The latter requirements are provided by
extras. If you want to use the feature in question, such as the worker-based
engine that uses `kombu`_, the `sqlalchemy`_ persistence backend or jobboards
which have an implementation built using `kazoo`_, you should add that
requirement(s) to your project or environment. For examples, to install the
worked-based engine:

.. code-block:: shell

    pip install taskflow[workers]

.. _kazoo: https://kazoo.readthedocs.io/en/latest/
.. _sqlalchemy: https://www.sqlalchemy.org/
.. _kombu: https://kombu.readthedocs.io/en/latest/
.. _eventlet: http://eventlet.net/
