# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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


def _exec(cmd, add_env=None):
    env = None
    if add_env:
        env = os.environ.copy()
        env.update(add_env)

    proc = subprocess.Popen(cmd, env=env, stdin=None,
                            stdout=subprocess.PIPE,
                            stderr=sys.stderr)

    stdout, stderr = proc.communicate()
    rc = proc.returncode
    if rc != 0:
        raise RuntimeError("Could not run %s [%s]", cmd, rc)
    print stdout


def _path_to(name):
    return os.path.abspath(os.path.join(os.path.dirname(__file__),
                                        'resume_many_flows', name))


def main():
    try:
        fd, db_path = tempfile.mkstemp(prefix='tf-resume-example')
        os.close(fd)
        backend_uri = 'sqlite:///%s' % db_path

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
        os.unlink(db_path)

if __name__ == '__main__':
    main()
