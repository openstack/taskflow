TaskFlow
========

A library to do [jobs, tasks, flows] in a HA manner using different backends to
be used with OpenStack projects.

* More information at http://wiki.openstack.org/wiki/TaskFlow

Join us
-------

- http://launchpad.net/taskflow

Testing and requirements
------------------------

### Requirements

Because TaskFlow has many optional (pluggable) parts like persistence
backends and engines, we decided to split our requirements into two
parts:
- things that are absolutely required by TaskFlow (you can't use
  TaskFlow without them) are put to `requirements.txt`;
- things that are required by some optional part of TaskFlow (you
  can use TaskFlow without them) are put to `optional-requirements.txt`;
  if you want to use the feature in question, you should add that
  requirements to your project or environment;
- as usual, things that required only for running tests are put
  to `test-requirements.txt`.

### Tox.ini

Our tox.ini describes several test environments that allow to test
TaskFlow with different python versions and sets of requirements
installed.

To generate tox.ini, use the `toxgen.py` script by first installing
[toxgen](https://pypi.python.org/pypi/toxgen/) and then provide that script
as input the `tox-tmpl.ini` file to generate the final `tox.ini` file.

*For example:*

    $ toxgen.py -i tox-tmpl.ini -o tox.ini


Documentation
-------------

http://wiki.openstack.org/wiki/TaskFlow

We also have sphinx documentation in `docs/source`. To build it,
run:

    $ python ./setup.py build_sphinx
