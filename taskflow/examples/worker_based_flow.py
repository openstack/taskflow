# -*- coding: utf-8 -*-

#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
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

import json
import os
import subprocess
import sys
import tempfile

self_dir = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, self_dir)

import example_utils  # noqa


def _path_to(name):
    return os.path.abspath(os.path.join(os.path.dirname(__file__),
                                        'worker_based', name))


def run_test(name, config):
    cmd = [sys.executable, _path_to(name), config]
    process = subprocess.Popen(cmd, stdin=None, stdout=subprocess.PIPE,
                               stderr=sys.stderr)
    return process, cmd


def main():
    tmp_path = None
    try:
        tmp_path = tempfile.mkdtemp(prefix='worker-based-example-')
        config = json.dumps({
            'transport': 'filesystem',
            'transport_options': {
                'data_folder_in': tmp_path,
                'data_folder_out': tmp_path
            }
        })

        print('Run worker.')
        worker_process, _ = run_test('worker.py', config)

        print('Run flow.')
        flow_process, flow_cmd = run_test('flow.py', config)
        stdout, _ = flow_process.communicate()
        rc = flow_process.returncode
        if rc != 0:
            raise RuntimeError("Could not run %s [%s]" % (flow_cmd, rc))
        print(stdout.decode())
        print('Flow finished.')

        print('Stop worker.')
        worker_process.terminate()

    finally:
        if tmp_path is not None:
            example_utils.rm_path(tmp_path)

if __name__ == '__main__':
    main()
