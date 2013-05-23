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

import functools
import unittest

from taskflow import exceptions as exc
from taskflow import states
from taskflow import wrappers

from taskflow.patterns import linear_flow as lw
from taskflow.tests import utils


class LinearFlowTest(unittest.TestCase):
    def make_reverting_task(self, token, blowup=False):

        def do_apply(token, context, *_args, **_kwargs):
            context[token] = 'passed'

        def do_revert(token, context, *_args, **_kwargs):
            context[token] = 'reverted'

        def blow_up(_context, *_args, **_kwargs):
            raise Exception("I blew up")

        if blowup:
            return wrappers.FunctorTask('task-%s' % (token),
                                        functools.partial(blow_up, token),
                                        utils.null_functor)
        else:
            return wrappers.FunctorTask('task-%s' % (token),
                                        functools.partial(do_apply, token),
                                        functools.partial(do_revert, token))

    def make_interrupt_task(self, token, wf):

        def do_interrupt(_context, *_args, **_kwargs):
            wf.interrupt()

        return wrappers.FunctorTask('task-%s' % (token),
                                    do_interrupt,
                                    utils.null_functor)

    def test_sad_flow_state_changes(self):
        wf = lw.Flow("the-test-action")
        flow_changes = []

        def flow_listener(_context, _wf, previous_state):
            flow_changes.append(previous_state)

        wf.listeners.append(flow_listener)
        wf.add(self.make_reverting_task(1, True))

        self.assertEquals(states.PENDING, wf.state)
        self.assertRaises(Exception, wf.run, {})

        expected_states = [
            states.PENDING,
            states.STARTED,
            states.RUNNING,
            states.REVERTING,
        ]
        self.assertEquals(expected_states, flow_changes)
        self.assertEquals(states.FAILURE, wf.state)

    def test_happy_flow_state_changes(self):
        wf = lw.Flow("the-test-action")
        flow_changes = []

        def flow_listener(_context, _wf, previous_state):
            flow_changes.append(previous_state)

        wf.listeners.append(flow_listener)
        wf.add(self.make_reverting_task(1))

        self.assertEquals(states.PENDING, wf.state)
        wf.run({})

        self.assertEquals([states.PENDING, states.STARTED, states.RUNNING],
                          flow_changes)

        self.assertEquals(states.SUCCESS, wf.state)

    def test_happy_flow(self):
        wf = lw.Flow("the-test-action")

        for i in range(0, 10):
            wf.add(self.make_reverting_task(i))

        run_context = {}
        wf.run(run_context)

        self.assertEquals(10, len(run_context))
        for _k, v in run_context.items():
            self.assertEquals('passed', v)

    def test_reverting_flow(self):
        wf = lw.Flow("the-test-action")
        wf.add(self.make_reverting_task(1))
        wf.add(self.make_reverting_task(2, True))

        run_context = {}
        self.assertRaises(Exception, wf.run, run_context)
        self.assertEquals('reverted', run_context[1])
        self.assertEquals(1, len(run_context))

    def test_not_satisfied_inputs_previous(self):
        wf = lw.Flow("the-test-action")

        def task_a(context, *args, **kwargs):
            pass

        def task_b(context, c, *args, **kwargs):
            pass

        wf.add(wrappers.FunctorTask(None, task_a, utils.null_functor,
                                    extract_requires=True))
        self.assertRaises(exc.InvalidStateException,
                          wf.add,
                          wrappers.FunctorTask(None, task_b,
                                               utils.null_functor,
                                               extract_requires=True))

    def test_not_satisfied_inputs_no_previous(self):
        wf = lw.Flow("the-test-action")

        def task_a(context, c, *args, **kwargs):
            pass

        self.assertRaises(exc.InvalidStateException,
                          wf.add,
                          wrappers.FunctorTask(None, task_a,
                                               utils.null_functor,
                                               extract_requires=True))

    def test_flow_add_order(self):
        wf = lw.Flow("the-test-action")

        wf.add(utils.ProvidesRequiresTask('test-1',
                                          requires=set(),
                                          provides=['a', 'b']))
        # This one should fail to add since it requires 'c'
        self.assertRaises(exc.InvalidStateException,
                          wf.add,
                          utils.ProvidesRequiresTask('test-2',
                                                     requires=['c'],
                                                     provides=[]))
        wf.add(utils.ProvidesRequiresTask('test-2',
                                          requires=['a', 'b'],
                                          provides=['c', 'd']))
        wf.add(utils.ProvidesRequiresTask('test-3',
                                          requires=['c', 'd'],
                                          provides=[]))
        wf.add(utils.ProvidesRequiresTask('test-4',
                                          requires=[],
                                          provides=['d']))
        wf.add(utils.ProvidesRequiresTask('test-5',
                                          requires=[],
                                          provides=['d']))
        wf.add(utils.ProvidesRequiresTask('test-6',
                                          requires=['d'],
                                          provides=[]))

    def test_interrupt_flow(self):
        wf = lw.Flow("the-int-action")

        result_storage = {}

        # If we interrupt we need to know how to resume so attach the needed
        # parts to do that...

        def result_fetcher(_ctx, _wf, task):
            if task.name in result_storage:
                return (True, result_storage.get(task.name))
            return (False, None)

        def task_listener(_ctx, state, _wf, task, result=None):
            if state not in (states.SUCCESS, states.FAILURE,):
                return
            if task.name not in result_storage:
                result_storage[task.name] = result

        wf.result_fetcher = result_fetcher
        wf.task_listeners.append(task_listener)

        wf.add(self.make_reverting_task(1))
        wf.add(self.make_interrupt_task(2, wf))
        wf.add(self.make_reverting_task(3))

        self.assertEquals(states.PENDING, wf.state)
        context = {}
        wf.run(context)

        # Interrupt should have been triggered after task 1
        self.assertEquals(1, len(context))
        self.assertEquals(states.INTERRUPTED, wf.state)

        # And now reset and resume.
        wf.reset()
        self.assertEquals(states.PENDING, wf.state)
        wf.run(context)
        self.assertEquals(2, len(context))

    def test_parent_reverting_flow(self):
        happy_wf = lw.Flow("the-happy-action")

        i = 0
        for i in range(0, 10):
            happy_wf.add(self.make_reverting_task(i))

        context = {}
        happy_wf.run(context)

        for (_k, v) in context.items():
            self.assertEquals('passed', v)

        baddy_wf = lw.Flow("the-bad-action", parents=[happy_wf])
        baddy_wf.add(self.make_reverting_task(i + 1))
        baddy_wf.add(self.make_reverting_task(i + 2, True))
        self.assertRaises(Exception, baddy_wf.run, context)

        for (_k, v) in context.items():
            self.assertEquals('reverted', v)
