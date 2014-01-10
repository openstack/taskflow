TaskFlow
========

A library to do [jobs, tasks, flows] in a HA manner using different backends to
be used with OpenStack projects.

* More information at http://wiki.openstack.org/wiki/TaskFlow

Join us
-------

- http://launchpad.net/taskflow

Help
----

### Tox.ini

To generate tox.ini, use the `toxgen.py` tool located in `tools/` and provide
that script as input the `tox-tmpl.ini` file to generate the final `tox.ini`
file.

For example:

    $ ./tools/toxgen.py -i tox-tmpl.ini -o tox.ini
