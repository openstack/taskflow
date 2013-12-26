#!/bin/bash

function usage {
  echo "Usage: $0 [OPTION]..."
  echo "Run Taskflow's test suite(s)"
  echo ""
  echo "  -f, --force              Force a clean re-build of the virtual environment. Useful when dependencies have been added."
  echo "  -p, --pep8               Just run pep8"
  echo "  -P, --no-pep8            Don't run static code checks"
  echo "  -v, --verbose            Increase verbosity of reporting output"
  echo "  -h, --help               Print this usage message"
  echo ""
  exit
}

function process_option {
  case "$1" in
    -h|--help) usage;;
    -p|--pep8) let just_pep8=1;;
    -P|--no-pep8) let no_pep8=1;;
    -f|--force) let force=1;;
    -v|--verbose) let verbose=1;;
    *) pos_args="$pos_args $1"
  esac
}

verbose=0
force=0
pos_args=""
just_pep8=0
no_pep8=0
tox_args=""
tox=""

for arg in "$@"; do
  process_option $arg
done

py=`which python`
if [ -z "$py" ]; then
    echo "Python is required to use $0"
    echo "Please install it via your distributions package management system."
    exit 1
fi

py_envs=`python -c 'import sys; print("py%s%s" % (sys.version_info[0:2]))'`
py_envs=${PY_ENVS:-$py_envs}

function run_tests {
    local tox_cmd="${tox} ${tox_args} -e $py_envs ${pos_args}"
    echo "Running tests for environments $py_envs via $tox_cmd"
    bash -c "$tox_cmd"
}

function run_flake8 {
  local tox_cmd="${tox} ${tox_args} -e pep8 ${pos_args}"
  echo "Running flake8 via $tox_cmd"
  bash -c "$tox_cmd"
}

if [ $force -eq 1 ]; then
  tox_args="$tox_args -r"
fi

if [ $verbose -eq 1 ]; then
  tox_args="$tox_args -v"
fi

tox=`which tox`
if [ -z "$tox" ]; then
    echo "Tox is required to use $0"
    echo "Please install it via \`pip\` or via your distributions" \
         "package management system."
    echo "Visit http://tox.readthedocs.org/ for additional installation" \
         "instructions."
    exit 1
fi

if [ $just_pep8 -eq 1 ]; then
  run_flake8
  exit
fi

run_tests || exit

if [ $no_pep8 -eq 0 ]; then
  run_flake8
fi
