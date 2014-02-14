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
sys.path.insert(0, self_dir)
sys.path.insert(0, example_dir)

import taskflow.engines

import example_utils  # noqa
import my_flows  # noqa


with example_utils.get_backend() as backend:
    engine = taskflow.engines.load_from_factory(my_flows.flow_factory,
                                                backend=backend)
    print('Running flow %s %s' % (engine.storage.flow_name,
                                  engine.storage.flow_uuid))
    engine.run()
