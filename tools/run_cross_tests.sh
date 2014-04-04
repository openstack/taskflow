#!/bin/bash
#
# Run cross-project tests
#
# Usage:
#
#   run_cross_tests.sh project_dir venv

# Fail the build if any command fails
set -e

project_dir="$1"
venv="$2"

if [ -z "$project_dir" -o -z "$venv" ]
then
    cat - <<EOF
ERROR: Missing argument(s)

Usage:

  $0 PROJECT_DIR VIRTUAL_ENV

Example, run the python 2.7 tests for python-neutronclient:

  $0 /opt/stack/python-neutronclient py27

EOF
    exit 1
fi

# Set up the virtualenv without running the tests
(cd $project_dir && tox --notest -e $venv)

tox_envbin=$project_dir/.tox/$venv/bin

our_name=$(python setup.py --name)

# Replace the pip-installed package with the version in our source
# tree. Look to see if we are already installed before trying to
# uninstall ourselves, to avoid failures from packages that do not use us
# yet.
if $tox_envbin/pip freeze | grep -q $our_name
then
    $tox_envbin/pip uninstall -y $our_name
fi
$tox_envbin/pip install -U .

# Run the tests
(cd $project_dir && tox -e $venv)
result=$?


# The below checks are modified from
# openstack-infra/config/modules/jenkins/files/slave_scripts/run-unittests.sh.

# They expect to be run in the project being tested.
cd $project_dir

echo "Begin pip freeze output from test virtualenv:"
echo "======================================================================"
.tox/$venv/bin/pip freeze
echo "======================================================================"

# We only want to run the next check if the tool is installed, so look
# for it before continuing.
if [ -f /usr/local/jenkins/slave_scripts/subunit2html.py -a -d ".testrepository" ] ; then
    if [ -f ".testrepository/0.2" ] ; then
        cp .testrepository/0.2 ./subunit_log.txt
    elif [ -f ".testrepository/0" ] ; then
        .tox/$venv/bin/subunit-1to2 < .testrepository/0 > ./subunit_log.txt
    fi
    .tox/$venv/bin/python /usr/local/jenkins/slave_scripts/subunit2html.py ./subunit_log.txt testr_results.html
    gzip -9 ./subunit_log.txt
    gzip -9 ./testr_results.html

    export PYTHON=.tox/$venv/bin/python
    set -e
    rancount=$(.tox/$venv/bin/testr last | sed -ne 's/Ran \([0-9]\+\).*tests in.*/\1/p')
    if [ "$rancount" -eq "0" ] ; then
        echo
        echo "Zero tests were run. At least one test should have been run."
        echo "Failing this test as a result"
        echo
        exit 1
    fi
fi

# If we make it this far, report status based on the tests that were
# run.
exit $result
