# -*- coding: utf-8 -*-

#    Copyright (C) 2015 Yahoo! Inc. All Rights Reserved.
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

from taskflow.listeners import base


def _freeze_it(values):
    """Freezes a set of values (handling none/empty nicely)."""
    if not values:
        return frozenset()
    else:
        return frozenset(values)


class CaptureListener(base.Listener):
    """A listener that captures transitions and saves them locally.

    NOTE(harlowja): this listener is *mainly* useful for testing (where it is
    useful to test the appropriate/expected transitions, produced results...
    occurred after engine running) but it could have other usages as well.

    :ivar values: Captured transitions + details (the result of
                  the :py:meth:`._format_capture` method) are stored into this
                  list (a previous list to append to may be provided using the
                  constructor keyword argument of the same name); by default
                  this stores tuples of the format ``(kind, state, details)``.
    """

    # Constant 'kind' strings used in the default capture formatting (to
    # identify what was captured); these are saved into the accumulated
    # values as the first index (so that your code may differentiate between
    # what was captured).

    #: Kind that denotes a 'flow' capture.
    FLOW = 'flow'

    #: Kind that denotes a 'task' capture.
    TASK = 'task'

    #: Kind that denotes a 'retry' capture.
    RETRY = 'retry'

    def __init__(self, engine,
                 task_listen_for=base.DEFAULT_LISTEN_FOR,
                 flow_listen_for=base.DEFAULT_LISTEN_FOR,
                 retry_listen_for=base.DEFAULT_LISTEN_FOR,
                 # Easily override what you want captured and where it
                 # should save into and what should be skipped...
                 capture_flow=True, capture_task=True, capture_retry=True,
                 # Skip capturing *all* tasks, all retries, all flows...
                 skip_tasks=None, skip_retries=None, skip_flows=None,
                 # Provide your own list (or previous list) to accumulate
                 # into...
                 values=None):
        super(CaptureListener, self).__init__(
            engine,
            task_listen_for=task_listen_for,
            flow_listen_for=flow_listen_for,
            retry_listen_for=retry_listen_for)
        self._capture_flow = capture_flow
        self._capture_task = capture_task
        self._capture_retry = capture_retry
        self._skip_tasks = _freeze_it(skip_tasks)
        self._skip_flows = _freeze_it(skip_flows)
        self._skip_retries = _freeze_it(skip_retries)
        if values is None:
            self.values = []
        else:
            self.values = values

    @staticmethod
    def _format_capture(kind, state, details):
        """Tweak what is saved according to your desire(s)."""
        return (kind, state, details)

    def _task_receiver(self, state, details):
        if self._capture_task:
            if details['task_name'] not in self._skip_tasks:
                self.values.append(self._format_capture(self.TASK,
                                                        state, details))

    def _retry_receiver(self, state, details):
        if self._capture_retry:
            if details['retry_name'] not in self._skip_retries:
                self.values.append(self._format_capture(self.RETRY,
                                                        state, details))

    def _flow_receiver(self, state, details):
        if self._capture_flow:
            if details['flow_name'] not in self._skip_flows:
                self.values.append(self._format_capture(self.FLOW,
                                                        state, details))
