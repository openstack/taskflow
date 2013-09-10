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

ARGS_KEY = '__args__'
KWARGS_KEY = '__kwargs__'
ORDER_KEY = '__order__'


def close_all(*args):
    for a in args:
        if not a:
            continue
        a.close()


def null_functor(*args, **kwargs):  # pylint: disable=W0613
    return None


def drain(lst):
    while len(lst):
        lst.pop()


class ProvidesRequiresTask(task.Task):
    def __init__(self, name, provides, requires, return_tuple=True):
        super(ProvidesRequiresTask, self).__init__(name=name,
                                                   provides=provides,
                                                   requires=requires)
        self.return_tuple = isinstance(provides, (tuple, list))

    def execute(self, context, *args, **kwargs):
        if ORDER_KEY not in context:
            context[ORDER_KEY] = []
        context[ORDER_KEY].append({
            'name': self.name,
            KWARGS_KEY: kwargs,
            ARGS_KEY: args,
        })
        if self.return_tuple:
            outs = []
            for i in xrange(0, len(self.provides)):
                outs.append(i)
            return tuple(outs)
        else:
            outs = {}
            for k in self.provides:
                outs[k] = k
            return outs


class DummyTask(task.Task):
    def execute(self, context, *args, **kwargs):
        pass
