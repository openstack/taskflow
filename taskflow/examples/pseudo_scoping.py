# -*- coding: utf-8 -*-

#    Copyright (C) 2014 Ivan Melnikov <iv at altlinux dot org>
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

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)

import taskflow.engines
from taskflow.patterns import linear_flow as lf
from taskflow import task

# INTRO: pseudo-scoping by adding prefixes

# Sometimes you need scoping -- e.g. for adding several
# similar subflows to one flow to do same stuff for different
# data. But current version of TaskFlow does not allow that
# directly, so you have to resort to some kind of trickery.
# One (and more or less recommended, if not the only) way of
# solving the problem is to transform every task name, it's
# provides and requires values -- e.g. by adding prefix to them.
# This example shows how this could be done.


# The example task is simple: for each specified person, fetch
# his or her phone number from phone book and call.


PHONE_BOOK = {
    'jim': '444',
    'joe': '555',
    'iv_m': '666',
    'josh': '777'
}


class FetchNumberTask(task.Task):
    """Task that fetches number from phone book."""

    default_provides = 'number'

    def execute(self, person):
        print('Fetching number for %s.' % person)
        return PHONE_BOOK[person.lower()]


class CallTask(task.Task):
    """Task that calls person by number."""

    def execute(self, person, number):
        print('Calling %s %s.' % (person, number))

# This is how it works for one person:

simple_flow = lf.Flow('simple one').add(
    FetchNumberTask(),
    CallTask())
print('Running simple flow:')
taskflow.engines.run(simple_flow, store={'person': 'Josh'})


# To call several people you'll need a factory function that will
# make a flow with given prefix for you. We need to add prefix
# to task names, their provides and requires values. For requires,
# we use `rebind` argument of task constructor.
def subflow_factory(prefix):
    def pr(what):
        return '%s-%s' % (prefix, what)

    return lf.Flow(pr('flow')).add(
        FetchNumberTask(pr('fetch'),
                        provides=pr('number'),
                        rebind=[pr('person')]),
        CallTask(pr('call'),
                 rebind=[pr('person'), pr('number')])
    )


def call_them_all():
    # Let's call them all. We need a flow:
    flow = lf.Flow('call-them-prefixed')

    # We'll also need to inject person names with prefixed argument
    # name to storage to satisfy task requirements.
    persons = {}

    for person in ('Jim', 'Joe', 'Josh'):
        prefix = person.lower()
        persons['%s-person' % prefix] = person
        flow.add(subflow_factory(prefix))
    taskflow.engines.run(flow, store=persons)

print('\nCalling many people using prefixed factory:')
call_them_all()
