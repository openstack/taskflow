# -*- coding: utf-8 -*-

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

import futurist

from taskflow.conductors.backends import impl_executor


class BlockingConductor(impl_executor.ExecutorConductor):
    """Blocking conductor that processes job(s) in a blocking manner."""

    MAX_SIMULTANEOUS_JOBS = 1
    """
    Default maximum number of jobs that can be in progress at the same time.
    """

    @staticmethod
    def _executor_factory():
        return futurist.SynchronousExecutor()

    def __init__(self, name, jobboard,
                 persistence=None, engine=None,
                 engine_options=None, wait_timeout=None,
                 log=None, max_simultaneous_jobs=MAX_SIMULTANEOUS_JOBS):
        super(BlockingConductor, self).__init__(
            name, jobboard,
            persistence=persistence, engine=engine,
            engine_options=engine_options,
            wait_timeout=wait_timeout, log=log,
            max_simultaneous_jobs=max_simultaneous_jobs)
