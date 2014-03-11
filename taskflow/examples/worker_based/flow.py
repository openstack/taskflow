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
import logging
import sys

import taskflow.engines
from taskflow.patterns import linear_flow as lf
from taskflow.tests import utils

LOG = logging.getLogger(__name__)


if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    engine_conf = {
        'engine': 'worker-based',
        'exchange': 'taskflow',
        'topics': ['test-topic'],
    }

    # parse command line
    try:
        arg = sys.argv[1]
    except IndexError:
        pass
    else:
        try:
            cfg = json.loads(arg)
        except ValueError:
            engine_conf.update(url=arg)
        else:
            engine_conf.update(cfg)
    finally:
        LOG.debug("Worker configuration: %s\n" %
                  json.dumps(engine_conf, sort_keys=True, indent=4))

    # create and run flow
    flow = lf.Flow('simple-linear').add(
        utils.TaskOneArgOneReturn(provides='result1'),
        utils.TaskMultiArgOneReturn(provides='result2')
    )
    eng = taskflow.engines.load(flow,
                                store=dict(x=111, y=222, z=333),
                                engine_conf=engine_conf)
    eng.run()
    print(json.dumps(eng.storage.fetch_all(), sort_keys=True))
