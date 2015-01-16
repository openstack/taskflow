# -*- coding: utf-8 -*-

#    Copyright (C) 2013 Yahoo! Inc. All Rights Reserved.
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

import os
import subprocess
import sys
import tempfile

self_dir = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, self_dir)

import example_utils  # noqa

# INTRO: In this example we create a common persistence database (sqlite based)
# and then we run a few set of processes which themselves use this persistence
# database, those processes 'crash' (in a simulated way) by exiting with a
# system error exception. After this occurs a few times we then activate a
# script which doesn't 'crash' and it will resume all the given engines flows
# that did not complete and run them to completion (instead of crashing).
#
# This shows how a set of tasks can be finished even after repeatedly being
# crashed, *crash resistance* if you may call it, due to the engine concept as
# well as the persistence layer which keeps track of the state a flow
# transitions through and persists the intermediary inputs and outputs and
# overall flow state.


def _exec(cmd, add_env=None):
    env = None
    if add_env:
        env = os.environ.copy()
        env.update(add_env)

    proc = subprocess.Popen(cmd, env=env, stdin=None,
                            stdout=subprocess.PIPE,
                            stderr=sys.stderr)

    stdout, _stderr = proc.communicate()
    rc = proc.returncode
    if rc != 0:
        raise RuntimeError("Could not run %s [%s]", cmd, rc)
    print(stdout.decode())


def _path_to(name):
    return os.path.abspath(os.path.join(os.path.dirname(__file__),
                                        'resume_many_flows', name))


def main():
    backend_uri = None
    tmp_path = None
    try:
        if example_utils.SQLALCHEMY_AVAILABLE:
            tmp_path = tempfile.mktemp(prefix='tf-resume-example')
            backend_uri = "sqlite:///%s" % (tmp_path)
        else:
            tmp_path = tempfile.mkdtemp(prefix='tf-resume-example')
            backend_uri = 'file:///%s' % (tmp_path)

        def run_example(name, add_env=None):
            _exec([sys.executable, _path_to(name), backend_uri], add_env)

        print('Run flow:')
        run_example('run_flow.py')

        print('\nRun flow, something happens:')
        run_example('run_flow.py', {'BOOM': 'exit please'})

        print('\nRun flow, something happens again:')
        run_example('run_flow.py', {'BOOM': 'exit please'})

        print('\nResuming all failed flows')
        run_example('resume_all.py')
    finally:
        if tmp_path:
            example_utils.rm_path(tmp_path)

if __name__ == '__main__':
    main()
