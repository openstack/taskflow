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

from taskflow.engines.action_engine import engine as eng
from taskflow import exceptions as exc
from taskflow.patterns import linear_flow as lw
from taskflow import states
from taskflow import task
from taskflow import test

from taskflow.tests import utils


class LinearFlowTest(test.TestCase):
    def _make_engine(self, flow):
        e = eng.SingleThreadedActionEngine(flow)
        e.storage.inject([('context', {})])
        e.compile()
        return e

    def test_result_access(self):

        def do_apply1(context):
            return [1, 2]

        wf = lw.Flow("the-test-action")
        wf.add(task.FunctorTask(do_apply1, provides=['a', 'b']))

        e = self._make_engine(wf)
        e.run()
        data = e.storage.fetch_all()
        self.assertIn('a', data)
        self.assertIn('b', data)
        self.assertEquals(2, data['b'])
        self.assertEquals(1, data['a'])

    def test_functor_flow(self):
        wf = lw.Flow("the-test-action")

        def do_apply1(context):
            context['1'] = True
            return ['a', 'b', 'c']

        def do_apply2(context, a, **kwargs):
            self.assertTrue('c' in kwargs)
            self.assertEquals('a', a)
            context['2'] = True

        wf.add(task.FunctorTask(do_apply1, provides=['a', 'b', 'c']))
        wf.add(task.FunctorTask(do_apply2, requires=set(['c'])))

        e = self._make_engine(wf)
        e.run()
        self.assertEquals(2, len(e.storage.fetch('context')))

    def test_sad_flow_state_changes(self):
        changes = []
        task_changes = []

        def listener(state, details):
            changes.append(state)

        def task_listener(state, details):
            if details.get('task_name') == 'blowup_1':
                task_changes.append(state)

        wf = lw.Flow("the-test-action")
        wf.add(utils.make_reverting_task(2, False))
        wf.add(utils.make_reverting_task(1, True))

        e = self._make_engine(wf)
        e.notifier.register('*', listener)
        e.task_notifier.register('*', task_listener)
        self.assertRaises(Exception, e.run)

        expected_states = [
            states.RUNNING,
            states.REVERTING,
            states.REVERTED,
            states.FAILURE,
        ]
        self.assertEquals(expected_states, changes)
        expected_states = [
            states.RUNNING,
            states.FAILURE,
            states.REVERTING,
            states.REVERTED,
            states.PENDING,
        ]
        self.assertEquals(expected_states, task_changes)
        context = e.storage.fetch('context')

        # Only 2 should have been reverted (which should have been
        # marked in the context as occuring).
        self.assertIn(2, context)
        self.assertEquals('reverted', context[2])
        self.assertNotIn(1, context)

    def test_happy_flow_state_changes(self):
        changes = []

        def listener(state, details):
            changes.append(state)

        wf = lw.Flow("the-test-action")
        wf.add(utils.make_reverting_task(1))

        e = self._make_engine(wf)
        e.notifier.register('*', listener)
        e.run()

        self.assertEquals([states.RUNNING, states.SUCCESS], changes)

    def test_happy_flow(self):
        wf = lw.Flow("the-test-action")
        for i in range(0, 10):
            wf.add(utils.make_reverting_task(i))

        e = self._make_engine(wf)
        capture_func, captured = self._capture_states()
        e.task_notifier.register('*', capture_func)
        e.run()

        context = e.storage.fetch('context')
        self.assertEquals(10, len(context))
        self.assertEquals(10, len(captured))
        for _k, v in context.items():
            self.assertEquals('passed', v)
        for _uuid, u_states in captured.items():
            self.assertEquals([states.RUNNING, states.SUCCESS], u_states)

    def _capture_states(self):
        capture_where = collections.defaultdict(list)

        def do_capture(state, details):
            task_uuid = details.get('task_uuid')
            if not task_uuid:
                return
            capture_where[task_uuid].append(state)

        return (do_capture, capture_where)

    def test_reverting_flow(self):
        wf = lw.Flow("the-test-action")
        wf.add(utils.make_reverting_task(1))
        wf.add(utils.make_reverting_task(2, True))

        capture_func, captured = self._capture_states()
        e = self._make_engine(wf)
        e.task_notifier.register('*', capture_func)

        self.assertRaises(Exception, e.run)

        run_context = e.storage.fetch('context')
        self.assertEquals('reverted', run_context[1])
        self.assertEquals(1, len(run_context))

        blowup_id = e.storage.get_uuid_by_name('blowup_2')
        happy_id = e.storage.get_uuid_by_name('do_apply_1')
        self.assertEquals(2, len(captured))
        self.assertIn(blowup_id, captured)

        expected_states = [states.RUNNING, states.FAILURE, states.REVERTING,
                           states.REVERTED, states.PENDING]
        self.assertEquals(expected_states, captured[blowup_id])

        expected_states = [states.RUNNING, states.SUCCESS, states.REVERTING,
                           states.REVERTED, states.PENDING]
        self.assertIn(happy_id, captured)
        self.assertEquals(expected_states, captured[happy_id])

    def test_not_satisfied_inputs(self):

        def task_a(context, *args, **kwargs):
            pass

        def task_b(context, c, *args, **kwargs):
            pass

        wf = lw.Flow("the-test-action")
        wf.add(task.FunctorTask(task_a))
        wf.add(task.FunctorTask(task_b))
        e = self._make_engine(wf)
        self.assertRaises(exc.MissingDependencies, e.run)

    def test_flow_bad_order(self):
        wf = lw.Flow("the-test-action")

        wf.add(utils.ProvidesRequiresTask('test-1',
                                          requires=set(),
                                          provides=['a', 'b']))

        # This one should fail to add since it requires 'c'
        no_req_task = utils.ProvidesRequiresTask('test-2', requires=['c'],
                                                 provides=[])
        wf.add(no_req_task)
        e = self._make_engine(wf)
        self.assertRaises(exc.MissingDependencies, e.run)

    def test_flow_set_order(self):
        wf = lw.Flow("the-test-action")
        wf.add(utils.ProvidesRequiresTask('test-1',
                                          requires=[],
                                          provides=set(['a', 'b'])))
        wf.add(utils.ProvidesRequiresTask('test-2',
                                          requires=set(['a', 'b']),
                                          provides=set([])))
        e = self._make_engine(wf)
        e.run()
        run_context = e.storage.fetch('context')
        ordering = run_context[utils.ORDER_KEY]
        self.assertEquals(2, len(ordering))
        self.assertEquals('test-1', ordering[0]['name'])
        self.assertEquals('test-2', ordering[1]['name'])
        self.assertEquals({'a': 'a', 'b': 'b'},
                          ordering[1][utils.KWARGS_KEY])
        self.assertEquals({},
                          ordering[0][utils.KWARGS_KEY])

    def test_flow_list_order(self):
        wf = lw.Flow("the-test-action")
        wf.add(utils.ProvidesRequiresTask('test-1',
                                          requires=[],
                                          provides=['a', 'b']))
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

        e = self._make_engine(wf)
        e.run()
        run_context = e.storage.fetch('context')
        ordering = run_context[utils.ORDER_KEY]
        for i, entry in enumerate(ordering):
            self.assertEquals('test-%s' % (i + 1), entry['name'])
