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

import logging
import os
import sys

logging.basicConfig(level=logging.ERROR)

self_dir = os.path.abspath(os.path.dirname(__file__))
top_dir = os.path.abspath(
    os.path.join(self_dir, os.pardir, os.pardir, os.pardir))
example_dir = os.path.abspath(os.path.join(self_dir, os.pardir))

sys.path.insert(0, top_dir)
sys.path.insert(0, example_dir)


import taskflow.engines
from taskflow import states

import example_utils  # noqa


FINISHED_STATES = (states.SUCCESS, states.FAILURE, states.REVERTED)


def resume(flowdetail, backend):
    print('Resuming flow %s %s' % (flowdetail.name, flowdetail.uuid))
    engine = taskflow.engines.load_from_detail(flow_detail=flowdetail,
                                               backend=backend)
    engine.run()


def main():
    with example_utils.get_backend() as backend:
        logbooks = list(backend.get_connection().get_logbooks())
        for lb in logbooks:
            for fd in lb:
                if fd.state not in FINISHED_STATES:
                    resume(fd, backend)


if __name__ == '__main__':
    main()
