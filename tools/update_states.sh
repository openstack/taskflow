#!/bin/bash

set -u
xsltproc=`which xsltproc`
if [ -z "$xsltproc" ]; then
    echo "Please install xsltproc before continuing."
    exit 1
fi

set -e
if [ ! -d "$PWD/.diagram-tools" ]; then
    git clone "https://github.com/vidarh/diagram-tools.git" "$PWD/.diagram-tools"
fi

script_dir=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
img_dir="$script_dir/../doc/source/img"

echo "---- Updating task state diagram ----"
python $script_dir/state_graph.py -t -f /tmp/states.svg
$xsltproc $PWD/.diagram-tools/notugly.xsl /tmp/states.svg > $img_dir/task_states.svg

echo "---- Updating flow state diagram ----"
python $script_dir/state_graph.py --flow -f /tmp/states.svg
$xsltproc $PWD/.diagram-tools/notugly.xsl /tmp/states.svg > $img_dir/flow_states.svg

echo "---- Updating engine state diagram ----"
python $script_dir/state_graph.py -e -f /tmp/states.svg
$xsltproc $PWD/.diagram-tools/notugly.xsl /tmp/states.svg > $img_dir/engine_states.svg

echo "---- Updating retry state diagram ----"
python $script_dir/state_graph.py -r -f /tmp/states.svg
$xsltproc $PWD/.diagram-tools/notugly.xsl /tmp/states.svg > $img_dir/retry_states.svg

echo "---- Updating wbe request state diagram ----"
python $script_dir/state_graph.py -w -f /tmp/states.svg
$xsltproc $PWD/.diagram-tools/notugly.xsl /tmp/states.svg > $img_dir/wbe_request_states.svg

echo "---- Updating job state diagram ----"
python $script_dir/state_graph.py -j -f /tmp/states.svg
$xsltproc $PWD/.diagram-tools/notugly.xsl /tmp/states.svg > $img_dir/job_states.svg
