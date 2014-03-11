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

from taskflow.engines.worker_based import worker as w

LOG = logging.getLogger(__name__)


if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    worker_conf = {
        'exchange': 'taskflow',
        'topic': 'test-topic',
        'tasks': [
            'taskflow.tests.utils:TaskOneArgOneReturn',
            'taskflow.tests.utils:TaskMultiArgOneReturn'
        ]
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
            worker_conf.update(url=arg)
        else:
            worker_conf.update(cfg)
    finally:
        LOG.debug("Worker configuration: %s\n" %
                  json.dumps(worker_conf, sort_keys=True, indent=4))

    # run worker
    worker = w.Worker(**worker_conf)
    try:
        worker.run()
    except KeyboardInterrupt:
        pass
