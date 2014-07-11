# -*- coding: utf-8 -*-

#    Copyright (C) 2012-2013 Yahoo! Inc. All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import logging
import os
import sys

logging.basicConfig(level=logging.ERROR)

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)

import taskflow.engines
from taskflow.patterns import graph_flow as gf
from taskflow import task

import example_utils as eu  # noqa


# In this example we demonstrate use of a target flow (a flow that only
# executes up to a specified target) to make an *oversimplified* pseudo
# build system. It pretends to compile all sources to object files and
# link them into an executable. It also can build docs, but this can be
# "switched off" via targeted flow special power -- ability to ignore
# all tasks not needed by its target.


class CompileTask(task.Task):
    """Pretends to take a source and make object file."""
    default_provides = 'object_filename'

    def execute(self, source_filename):
        object_filename = '%s.o' % os.path.splitext(source_filename)[0]
        print('Compiling %s into %s'
              % (source_filename, object_filename))
        return object_filename


class LinkTask(task.Task):
    """Pretends to link executable form several object files."""
    default_provides = 'executable'

    def __init__(self, executable_path, *args, **kwargs):
        super(LinkTask, self).__init__(*args, **kwargs)
        self._executable_path = executable_path

    def execute(self, **kwargs):
        object_filenames = list(kwargs.values())
        print('Linking executable %s from files %s'
              % (self._executable_path,
                 ', '.join(object_filenames)))
        return self._executable_path


class BuildDocsTask(task.Task):
    """Pretends to build docs from sources."""
    default_provides = 'docs'

    def execute(self, **kwargs):
        for source_filename in kwargs.values():
            print("Building docs for %s" % source_filename)
        return 'docs'


def make_flow_and_store(source_files, executable_only=False):
    flow = gf.TargetedFlow('build-flow')
    object_targets = []
    store = {}
    for source in source_files:
        source_stored = '%s-source' % source
        object_stored = '%s-object' % source
        store[source_stored] = source
        object_targets.append(object_stored)
        flow.add(CompileTask(name='compile-%s' % source,
                             rebind={'source_filename': source_stored},
                             provides=object_stored))
    flow.add(BuildDocsTask(requires=list(store.keys())))

    # Try this to see executable_only switch broken:
    object_targets.append('docs')
    link_task = LinkTask('build/executable', requires=object_targets)
    flow.add(link_task)
    if executable_only:
        flow.set_target(link_task)
    return flow, store


if __name__ == "__main__":
    SOURCE_FILES = ['first.c', 'second.cpp', 'main.cpp']
    eu.print_wrapped('Running all tasks:')
    flow, store = make_flow_and_store(SOURCE_FILES)
    taskflow.engines.run(flow, store=store)

    eu.print_wrapped('Building executable, no docs:')
    flow, store = make_flow_and_store(SOURCE_FILES, executable_only=True)
    taskflow.engines.run(flow, store=store)
