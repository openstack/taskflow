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

import collections

from taskflow import decorators
from taskflow import exceptions as exc
from taskflow import states
from taskflow import test

from taskflow.patterns import linear_flow as lw
from taskflow.tests import utils


class LinearFlowTest(test.TestCase):
    def make_reverting_task(self, token, blowup=False):

        def do_revert(context, *args, **kwargs):
            context[token] = 'reverted'

        if blowup:
            @decorators.task(name='blowup %s' % token)
            def blow_up(context, *args, **kwargs):
                raise Exception("I blew up")
            return blow_up
        else:
            @decorators.task(revert_with=do_revert,
                             name='do_apply %s' % token)
            def do_apply(context, *args, **kwargs):
                context[token] = 'passed'
            return do_apply

    def make_interrupt_task(self, wf):

        @decorators.task
        def do_interrupt(context, *args, **kwargs):
            wf.interrupt()

        return do_interrupt

    def test_result_access(self):
        wf = lw.Flow("the-test-action")

        @decorators.task
        def do_apply1(context):
            return [1, 2]

        result_id = wf.add(do_apply1)
        ctx = {}
        wf.run(ctx)
        self.assertTrue(result_id in wf.results)
        self.assertEquals([1, 2], wf.results[result_id])

    def test_functor_flow(self):
        wf = lw.Flow("the-test-action")

        @decorators.task(provides=['a', 'b', 'c'])
        def do_apply1(context):
            context['1'] = True
            return {
                'a': 1,
                'b': 2,
                'c': 3,
            }

        @decorators.task(requires=['c', 'a'], auto_extract=False)
        def do_apply2(context, **kwargs):
            self.assertTrue('c' in kwargs)
            self.assertEquals(1, kwargs['a'])
            context['2'] = True

        ctx = {}
        wf.add(do_apply1)
        wf.add(do_apply2)
        wf.run(ctx)
        self.assertEquals(2, len(ctx))

    def test_sad_flow_state_changes(self):
        wf = lw.Flow("the-test-action")
        flow_changes = []

        def flow_listener(state, details):
            flow_changes.append(details['old_state'])

        wf.notifier.register('*', flow_listener)
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

        def flow_listener(state, details):
            flow_changes.append(details['old_state'])

        wf.notifier.register('*', flow_listener)
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
        capture_func, captured = self._capture_states()
        wf.task_notifier.register('*', capture_func)
        wf.run(run_context)

        self.assertEquals(10, len(run_context))
        self.assertEquals(10, len(captured))
        for _k, v in run_context.items():
            self.assertEquals('passed', v)
        for _uuid, u_states in captured.items():
            self.assertEquals([states.STARTED, states.SUCCESS], u_states)

    def _capture_states(self):
        capture_where = collections.defaultdict(list)

        def do_capture(state, details):
            runner = details.get('runner')
            if not runner:
                return
            capture_where[runner.uuid].append(state)

        return (do_capture, capture_where)

    def test_reverting_flow(self):
        wf = lw.Flow("the-test-action")
        ok_uuid = wf.add(self.make_reverting_task(1))
        broke_uuid = wf.add(self.make_reverting_task(2, True))
        capture_func, captured = self._capture_states()
        wf.task_notifier.register('*', capture_func)

        run_context = {}
        self.assertRaises(Exception, wf.run, run_context)
        self.assertEquals('reverted', run_context[1])
        self.assertEquals(1, len(run_context))
        self.assertEquals([states.STARTED, states.SUCCESS, states.REVERTING,
                           states.REVERTED], captured[ok_uuid])
        self.assertEquals([states.STARTED, states.FAILURE, states.REVERTING,
                           states.REVERTED], captured[broke_uuid])

    def test_not_satisfied_inputs_previous(self):
        wf = lw.Flow("the-test-action")

        @decorators.task
        def task_a(context, *args, **kwargs):
            pass

        @decorators.task
        def task_b(context, c, *args, **kwargs):
            pass

        wf.add(task_a)
        wf.add(task_b)
        self.assertRaises(exc.InvalidStateException, wf.run, {})

    def test_not_satisfied_inputs_no_previous(self):
        wf = lw.Flow("the-test-action")

        @decorators.task
        def task_a(context, c, *args, **kwargs):
            pass

        wf.add(task_a)
        self.assertRaises(exc.InvalidStateException, wf.run, {})

    def test_flow_add_order(self):
        wf = lw.Flow("the-test-action")

        wf.add(utils.ProvidesRequiresTask('test-1',
                                          requires=set(),
                                          provides=['a', 'b']))
        # This one should fail to add since it requires 'c'
        uuid = wf.add(utils.ProvidesRequiresTask('test-2',
                                                 requires=['c'],
                                                 provides=[]))
        self.assertRaises(exc.InvalidStateException, wf.run, {})
        wf.remove(uuid)

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
        wf.reset()
        wf.run({})

#   def test_interrupt_flow(self):
#       wf = lw.Flow("the-int-action")
#
#       # If we interrupt we need to know how to resume so attach the needed
#       # parts to do that...
#       tracker = lr.Resumption(memory.MemoryLogBook())
#       tracker.record_for(wf)
#       wf.resumer = tracker
#
#       wf.add(self.make_reverting_task(1))
#       wf.add(self.make_interrupt_task(wf))
#       wf.add(self.make_reverting_task(2))
#
#       self.assertEquals(states.PENDING, wf.state)
#       context = {}
#       wf.run(context)
#
#       # Interrupt should have been triggered after task 1
#       self.assertEquals(1, len(context))
#       self.assertEquals(states.INTERRUPTED, wf.state)
#
#       # And now reset and resume.
#       wf.reset()
#       tracker.record_for(wf)
#       wf.resumer = tracker
#       self.assertEquals(states.PENDING, wf.state)
#       wf.run(context)
#       self.assertEquals(2, len(context))

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
