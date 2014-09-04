TaskFlow
========

A library to do [jobs, tasks, flows] in a highly available, easy to understand
and declarative manner (and more!) to be used with OpenStack and other
projects.

- More information can be found by referring to the `developer documentation`_.

Join us
-------

- http://launchpad.net/taskflow

Testing and requirements
------------------------

Requirements
~~~~~~~~~~~~

Because TaskFlow has many optional (pluggable) parts like persistence
backends and engines, we decided to split our requirements into two
parts: - things that are absolutely required by TaskFlow (you can't use
TaskFlow without them) are put into ``requirements-pyN.txt`` (``N`` being the
Python *major* version number used to install the package); - things that are
required by some optional part of TaskFlow (you can use TaskFlow without
them) are put into ``optional-requirements.txt``; if you want to use the
feature in question, you should add that requirements to your project or
environment; - as usual, things that required only for running tests are
put into ``test-requirements.txt``.

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

.. _tox: http://testrun.org/tox/latest/
.. _developer documentation: http://docs.openstack.org/developer/taskflow/
