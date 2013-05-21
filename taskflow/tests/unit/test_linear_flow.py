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

from taskflow import states
from taskflow import task
from taskflow import wrappers

from taskflow.patterns import linear_flow as lw


def null_functor(*args, **kwargs):
    return None


class LinearFlowTest(unittest.TestCase):
    def makeRevertingTask(self, token, blowup=False):

        def do_apply(token, context, *args, **kwargs):
            context[token] = 'passed'

        def do_revert(token, context, *args, **kwargs):
            context[token] = 'reverted'

        def blow_up(context, *args, **kwargs):
            raise Exception("I blew up")

        if blowup:
            return wrappers.FunctorTask('task-%s' % (token),
                                        functools.partial(blow_up, token),
                                        null_functor)
        else:
            return wrappers.FunctorTask('task-%s' % (token),
                                        functools.partial(do_apply, token),
                                        functools.partial(do_revert, token))

    def makeInterruptTask(self, token, wf):

        def do_interrupt(token, context, *args, **kwargs):
            wf.interrupt()

        return wrappers.FunctorTask('task-%s' % (token),
                                    functools.partial(do_interrupt, token),
                                    null_functor)

    def testSadFlowStateChanges(self):
        wf = lw.Flow("the-test-action")
        flow_changes = []

        def flow_listener(context, wf, previous_state):
            flow_changes.append(previous_state)

        wf.listeners.append(flow_listener)
        wf.add(self.makeRevertingTask(1, True))

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

    def testHappyFlowStateChanges(self):
        wf = lw.Flow("the-test-action")
        flow_changes = []

        def flow_listener(context, wf, previous_state):
            flow_changes.append(previous_state)

        wf.listeners.append(flow_listener)
        wf.add(self.makeRevertingTask(1))

        self.assertEquals(states.PENDING, wf.state)
        wf.run({})

        self.assertEquals([states.PENDING, states.STARTED, states.RUNNING],
                          flow_changes)

        self.assertEquals(states.SUCCESS, wf.state)

    def testHappyPath(self):
        wf = lw.Flow("the-test-action")

        for i in range(0, 10):
            wf.add(self.makeRevertingTask(i))

        run_context = {}
        wf.run(run_context)

        self.assertEquals(10, len(run_context))
        for _k, v in run_context.items():
            self.assertEquals('passed', v)

    def testRevertingPath(self):
        wf = lw.Flow("the-test-action")
        wf.add(self.makeRevertingTask(1))
        wf.add(self.makeRevertingTask(2, True))

        run_context = {}
        self.assertRaises(Exception, wf.run, run_context)
        self.assertEquals('reverted', run_context[1])
        self.assertEquals(1, len(run_context))

    def testInterruptPath(self):
        wf = lw.Flow("the-int-action")

        result_storage = {}

        # If we interrupt we need to know how to resume so attach the needed
        # parts to do that...

        def result_fetcher(ctx, wf, task):
            if task.name in result_storage:
                return (True, result_storage.get(task.name))
            return (False, None)

        def task_listener(ctx, state, wf, task, result=None):
            if state not in (states.SUCCESS, states.FAILURE,):
                return
            if task.name not in result_storage:
                result_storage[task.name] = result

        wf.result_fetcher = result_fetcher
        wf.task_listeners.append(task_listener)

        wf.add(self.makeRevertingTask(1))
        wf.add(self.makeInterruptTask(2, wf))
        wf.add(self.makeRevertingTask(3))

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

    def testParentRevertingPath(self):
        happy_wf = lw.Flow("the-happy-action")
        for i in range(0, 10):
            happy_wf.add(self.makeRevertingTask(i))
        context = {}
        happy_wf.run(context)

        for (_k, v) in context.items():
            self.assertEquals('passed', v)

        baddy_wf = lw.Flow("the-bad-action", parents=[happy_wf])
        baddy_wf.add(self.makeRevertingTask(i + 1))
        baddy_wf.add(self.makeRevertingTask(i + 2, True))
        self.assertRaises(Exception, baddy_wf.run, context)

        for (_k, v) in context.items():
            self.assertEquals('reverted', v)
