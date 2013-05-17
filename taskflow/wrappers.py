# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
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

from taskflow import task


class FunctorTask(task.Task):
    """A simple task that can wrap two given functions and allow them to be
    in combination used in apply and reverting a given task. Useful for
    situations where existing functions already are in place and you just want
    to wrap them up."""

    def __init__(self, name, apply_functor, revert_functor):
        super(FunctorTask, self).__init__(name)
        if not self.name:
            self.name = "%s_%s" % (apply_functor.__name__,
                                   revert_functor.__name__)
        self._apply_functor = apply_functor
        self._revert_functor = revert_functor

    def apply(self, context, *args, **kwargs):
        return self._apply_functor(context, *args, **kwargs)

    def revert(self, context, result, cause):
        return self._revert_functor(context, result, cause)
