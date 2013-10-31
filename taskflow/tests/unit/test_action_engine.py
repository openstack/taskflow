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

import contextlib
import networkx

from concurrent import futures

from taskflow.patterns import graph_flow as gf
from taskflow.patterns import linear_flow as lf
from taskflow.patterns import unordered_flow as uf

import taskflow.engines

from taskflow.engines.action_engine import engine as eng
from taskflow.persistence import logbook
from taskflow import states
from taskflow import task
from taskflow import test
from taskflow.tests import utils
from taskflow.utils import misc
from taskflow.utils import persistence_utils as p_utils


class EngineTaskTest(utils.EngineTestBase):

    def test_run_task_as_flow(self):
        flow = utils.SaveOrderTask(self.values, name='task1')
        engine = self._make_engine(flow)
        engine.run()
        self.assertEqual(self.values, ['task1'])

    @staticmethod
    def _callback(state, values, details):
        name = details.get('task_name', '<unknown>')
        values.append('%s %s' % (name, state))

    @staticmethod
    def _flow_callback(state, values, details):
        values.append('flow %s' % state)

    def test_run_task_with_notifications(self):
        flow = utils.SaveOrderTask(self.values, name='task1')
        engine = self._make_engine(flow)
        engine.notifier.register('*', self._flow_callback,
                                 kwargs={'values': self.values})
        engine.task_notifier.register('*', self._callback,
                                      kwargs={'values': self.values})
        engine.run()
        self.assertEqual(self.values,
                         ['flow RUNNING',
                          'task1 RUNNING',
                          'task1',
                          'task1 SUCCESS',
                          'flow SUCCESS'])

    def test_failing_task_with_notifications(self):
        flow = utils.FailingTask(self.values, 'fail')
        engine = self._make_engine(flow)
        engine.notifier.register('*', self._flow_callback,
                                 kwargs={'values': self.values})
        engine.task_notifier.register('*', self._callback,
                                      kwargs={'values': self.values})
        expected = ['flow RUNNING',
                    'fail RUNNING',
                    'fail FAILURE',
                    'flow FAILURE',
                    'flow REVERTING',
                    'fail REVERTING',
                    'fail reverted(Failure: RuntimeError: Woot!)',
                    'fail REVERTED',
                    'flow REVERTED']
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            engine.run()
        self.assertEqual(self.values, expected)
        self.assertEqual(engine.storage.get_flow_state(), states.REVERTED)

        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            engine.run()
        now_expected = expected + ['fail PENDING', 'flow PENDING'] + expected
        self.assertEqual(self.values, now_expected)
        self.assertEqual(engine.storage.get_flow_state(), states.REVERTED)

    def test_invalid_flow_raises(self):
        value = 'i am string, not task/flow, sorry'
        with self.assertRaises(TypeError) as err:
            engine = self._make_engine(value)
            engine.compile()
        self.assertIn(value, str(err.exception))

    def test_invalid_flow_raises_from_run(self):
        value = 'i am string, not task/flow, sorry'
        with self.assertRaises(TypeError) as err:
            engine = self._make_engine(value)
            engine.run()
        self.assertIn(value, str(err.exception))


class EngineLinearFlowTest(utils.EngineTestBase):

    def test_sequential_flow_one_task(self):
        flow = lf.Flow('flow-1').add(
            utils.SaveOrderTask(self.values, name='task1')
        )
        self._make_engine(flow).run()
        self.assertEqual(self.values, ['task1'])

    def test_sequential_flow_two_tasks(self):
        flow = lf.Flow('flow-2').add(
            utils.SaveOrderTask(self.values, name='task1'),
            utils.SaveOrderTask(self.values, name='task2')
        )
        self._make_engine(flow).run()
        self.assertEqual(self.values, ['task1', 'task2'])
        self.assertEqual(len(flow), 2)

    def test_revert_removes_data(self):
        flow = lf.Flow('revert-removes').add(
            utils.TaskOneReturn(provides='one'),
            utils.TaskMultiReturn(provides=('a', 'b', 'c')),
            utils.FailingTask(name='fail')
        )
        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            engine.run()
        self.assertEqual(engine.storage.fetch_all(), {})

    def test_sequential_flow_nested_blocks(self):
        flow = lf.Flow('nested-1').add(
            utils.SaveOrderTask(self.values, 'task1'),
            lf.Flow('inner-1').add(
                utils.SaveOrderTask(self.values, 'task2')
            )
        )
        self._make_engine(flow).run()
        self.assertEqual(self.values, ['task1', 'task2'])

    def test_revert_exception_is_reraised(self):
        flow = lf.Flow('revert-1').add(
            utils.NastyTask(),
            utils.FailingTask(name='fail')
        )
        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Gotcha'):
            engine.run()

    def test_revert_not_run_task_is_not_reverted(self):
        flow = lf.Flow('revert-not-run').add(
            utils.FailingTask(self.values, 'fail'),
            utils.NeverRunningTask(),
        )
        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            engine.run()
        self.assertEqual(
            self.values,
            ['fail reverted(Failure: RuntimeError: Woot!)'])

    def test_correctly_reverts_children(self):
        flow = lf.Flow('root-1').add(
            utils.SaveOrderTask(self.values, 'task1'),
            lf.Flow('child-1').add(
                utils.SaveOrderTask(self.values, 'task2'),
                utils.FailingTask(self.values, 'fail')
            )
        )
        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            engine.run()
        self.assertEqual(
            self.values,
            ['task1', 'task2',
             'fail reverted(Failure: RuntimeError: Woot!)',
             'task2 reverted(5)', 'task1 reverted(5)'])

    def test_flow_failures_are_passed_to_revert(self):
        class CheckingTask(task.Task):
            def execute(m_self):
                return 'RESULT'

            def revert(m_self, result, flow_failures):
                self.assertEqual(result, 'RESULT')
                self.assertEqual(flow_failures.keys(), ['fail1'])
                fail = flow_failures['fail1']
                self.assertIsInstance(fail, misc.Failure)
                self.assertEqual(str(fail), 'Failure: RuntimeError: Woot!')

        flow = lf.Flow('test').add(
            CheckingTask(),
            utils.FailingTask(self.values, 'fail1')
        )
        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            engine.run()


class EngineParallelFlowTest(utils.EngineTestBase):

    def test_parallel_flow_one_task(self):
        flow = uf.Flow('p-1').add(
            utils.SaveOrderTask(self.values, name='task1', sleep=0.01)
        )
        self._make_engine(flow).run()
        self.assertEqual(self.values, ['task1'])

    def test_parallel_flow_two_tasks(self):
        flow = uf.Flow('p-2').add(
            utils.SaveOrderTask(self.values, name='task1', sleep=0.01),
            utils.SaveOrderTask(self.values, name='task2', sleep=0.01)
        )
        self._make_engine(flow).run()

        result = set(self.values)
        self.assertEqual(result, set(['task1', 'task2']))
        self.assertEqual(len(flow), 2)

    def test_parallel_revert_common(self):
        flow = uf.Flow('p-r-3').add(
            utils.TaskNoRequiresNoReturns(name='task1'),
            utils.FailingTask(sleep=0.01),
            utils.TaskNoRequiresNoReturns(name='task2')
        )
        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            engine.run()

    def test_parallel_revert_exception_is_reraised(self):
        # NOTE(imelnikov): if we put NastyTask and FailingTask
        # into the same unordered flow, it is not guaranteed
        # that NastyTask execution would be attempted before
        # FailingTask fails.
        flow = lf.Flow('p-r-r-l').add(
            uf.Flow('p-r-r').add(
                utils.TaskNoRequiresNoReturns(name='task1'),
                utils.NastyTask()
            ),
            utils.FailingTask(self.values, sleep=0.1)
        )
        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Gotcha'):
            engine.run()

    def test_sequential_flow_two_tasks_with_resumption(self):
        flow = lf.Flow('lf-2-r').add(
            utils.SaveOrderTask(self.values, name='task1', provides='x1'),
            utils.SaveOrderTask(self.values, name='task2', provides='x2')
        )

        # Create FlowDetail as if we already run task1
        _lb, fd = p_utils.temporary_flow_detail(self.backend)
        td = logbook.TaskDetail(name='task1', uuid='42')
        td.state = states.SUCCESS
        td.results = 17
        fd.add(td)

        with contextlib.closing(self.backend.get_connection()) as conn:
            fd.update(conn.update_flow_details(fd))
            td.update(conn.update_task_details(td))

        engine = self._make_engine(flow, fd)
        engine.run()
        self.assertEqual(self.values, ['task2'])
        self.assertEqual(engine.storage.fetch_all(),
                         {'x1': 17, 'x2': 5})

    def test_parallel_revert_specific(self):
        flow = uf.Flow('p-r-r').add(
            utils.SaveOrderTask(self.values, name='task1', sleep=0.01),
            utils.FailingTask(sleep=0.01),
            utils.SaveOrderTask(self.values, name='task2', sleep=0.01)
        )
        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            engine.run()
        result = set(self.values)
        # NOTE(harlowja): task 1/2 may or may not have executed, even with the
        # sleeps due to the fact that the above is an unordered flow.
        possible_result = set(['task1', 'task2',
                               'task2 reverted(5)', 'task1 reverted(5)'])
        self.assertIsSubset(possible_result, result)

    def test_parallel_revert_exception_is_reraised_(self):
        flow = lf.Flow('p-r-reraise').add(
            utils.SaveOrderTask(self.values, name='task1', sleep=0.01),
            utils.NastyTask(),
            utils.FailingTask(sleep=0.01),
            utils.SaveOrderTask(self.values,
                                name='task2')  # this should not get reverted
        )
        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Gotcha'):
            engine.run()
        result = set(self.values)
        self.assertEqual(result, set(['task1']))

    def test_nested_parallel_revert_exception_is_reraised(self):
        flow = uf.Flow('p-root').add(
            utils.SaveOrderTask(self.values, name='task1'),
            utils.SaveOrderTask(self.values, name='task2'),
            lf.Flow('p-inner').add(
                utils.SaveOrderTask(self.values, name='task3', sleep=0.1),
                utils.NastyTask(),
                utils.FailingTask(sleep=0.01)
            )
        )
        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Gotcha'):
            engine.run()
        result = set(self.values)
        # Task1, task2 may *not* have executed and also may have *not* reverted
        # since the above is an unordered flow so take that into account by
        # ensuring that the superset is matched.
        possible_result = set(['task1', 'task1 reverted(5)',
                               'task2', 'task2 reverted(5)',
                               'task3', 'task3 reverted(5)'])
        self.assertIsSubset(possible_result, result)

    def test_parallel_revert_exception_do_not_revert_linear_tasks(self):
        flow = lf.Flow('l-root').add(
            utils.SaveOrderTask(self.values, name='task1'),
            utils.SaveOrderTask(self.values, name='task2'),
            uf.Flow('p-inner').add(
                utils.SaveOrderTask(self.values, name='task3', sleep=0.1),
                utils.NastyTask(),
                utils.FailingTask(sleep=0.01)
            )
        )
        engine = self._make_engine(flow)
        # Depending on when (and if failing task) is executed the exception
        # raised could be either woot or gotcha since the above unordered
        # sub-flow does not guarantee that the ordering will be maintained,
        # even with sleeping.
        was_nasty = False
        try:
            engine.run()
            self.assertTrue(False)
        except RuntimeError as e:
            self.assertRegexpMatches(str(e), '^Gotcha|^Woot')
            if 'Gotcha!' in str(e):
                was_nasty = True
        result = set(self.values)
        possible_result = set(['task1', 'task2',
                               'task3', 'task3 reverted(5)'])
        if not was_nasty:
            possible_result.update(['task1 reverted(5)', 'task2 reverted(5)'])
        self.assertIsSubset(possible_result, result)
        # If the nasty task killed reverting, then task1 and task2 should not
        # have reverted, but if the failing task stopped execution then task1
        # and task2 should have reverted.
        if was_nasty:
            must_not_have = ['task1 reverted(5)', 'task2 reverted(5)']
            for r in must_not_have:
                self.assertNotIn(r, result)
        else:
            must_have = ['task1 reverted(5)', 'task2 reverted(5)']
            for r in must_have:
                self.assertIn(r, result)

    def test_parallel_nested_to_linear_revert(self):
        flow = lf.Flow('l-root').add(
            utils.SaveOrderTask(self.values, name='task1'),
            utils.SaveOrderTask(self.values, name='task2'),
            uf.Flow('p-inner').add(
                utils.SaveOrderTask(self.values, name='task3', sleep=0.1),
                utils.FailingTask(sleep=0.01)
            )
        )
        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            engine.run()
        result = set(self.values)
        # Task3 may or may not have executed, depending on scheduling and
        # task ordering selection, so it may or may not exist in the result set
        possible_result = set(['task1', 'task1 reverted(5)',
                               'task2', 'task2 reverted(5)',
                               'task3', 'task3 reverted(5)'])
        self.assertIsSubset(possible_result, result)
        # These must exist, since the linearity of the linear flow ensures
        # that they were executed first.
        must_have = ['task1', 'task1 reverted(5)',
                     'task2', 'task2 reverted(5)']
        for r in must_have:
            self.assertIn(r, result)

    def test_linear_nested_to_parallel_revert(self):
        flow = uf.Flow('p-root').add(
            utils.SaveOrderTask(self.values, name='task1'),
            utils.SaveOrderTask(self.values, name='task2'),
            lf.Flow('l-inner').add(
                utils.SaveOrderTask(self.values, name='task3', sleep=0.1),
                utils.FailingTask(self.values, name='fail', sleep=0.01)
            )
        )
        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            engine.run()
        result = set(self.values)
        # Since this is an unordered flow we can not guarantee that task1 or
        # task2 will exist and be reverted, although they may exist depending
        # on how the OS thread scheduling and execution graph algorithm...
        possible_result = set([
            'task1', 'task1 reverted(5)',
            'task2', 'task2 reverted(5)',
            'task3', 'task3 reverted(5)',
            'fail reverted(Failure: RuntimeError: Woot!)'
        ])
        self.assertIsSubset(possible_result, result)

    def test_linear_nested_to_parallel_revert_exception(self):
        flow = uf.Flow('p-root').add(
            utils.SaveOrderTask(self.values, name='task1', sleep=0.01),
            utils.SaveOrderTask(self.values, name='task2', sleep=0.01),
            lf.Flow('l-inner').add(
                utils.SaveOrderTask(self.values, name='task3'),
                utils.NastyTask(),
                utils.FailingTask(sleep=0.01)
            )
        )
        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Gotcha'):
            engine.run()
        result = set(self.values)
        possible_result = set(['task1', 'task1 reverted(5)',
                               'task2', 'task2 reverted(5)',
                               'task3'])
        self.assertIsSubset(possible_result, result)


class EngineGraphFlowTest(utils.EngineTestBase):

    def test_graph_flow_one_task(self):
        flow = gf.Flow('g-1').add(
            utils.SaveOrderTask(self.values, name='task1')
        )
        self._make_engine(flow).run()
        self.assertEqual(self.values, ['task1'])

    def test_graph_flow_two_independent_tasks(self):
        flow = gf.Flow('g-2').add(
            utils.SaveOrderTask(self.values, name='task1'),
            utils.SaveOrderTask(self.values, name='task2')
        )
        self._make_engine(flow).run()
        self.assertEqual(set(self.values), set(['task1', 'task2']))
        self.assertEqual(len(flow), 2)

    def test_graph_flow_two_tasks(self):
        flow = gf.Flow('g-1-1').add(
            utils.SaveOrderTask(self.values, name='task2', requires=['a']),
            utils.SaveOrderTask(self.values, name='task1', provides='a')
        )
        self._make_engine(flow).run()
        self.assertEqual(self.values, ['task1', 'task2'])

    def test_graph_flow_four_tasks_added_separately(self):
        flow = (gf.Flow('g-4')
                .add(utils.SaveOrderTask(self.values, name='task4',
                                         provides='d', requires=['c']))
                .add(utils.SaveOrderTask(self.values, name='task2',
                                         provides='b', requires=['a']))
                .add(utils.SaveOrderTask(self.values, name='task3',
                                         provides='c', requires=['b']))
                .add(utils.SaveOrderTask(self.values, name='task1',
                                         provides='a'))
                )
        self._make_engine(flow).run()
        self.assertEqual(self.values, ['task1', 'task2', 'task3', 'task4'])

    def test_graph_flow_four_tasks_revert(self):
        flow = gf.Flow('g-4-failing').add(
            utils.SaveOrderTask(self.values, name='task4',
                                provides='d', requires=['c']),
            utils.SaveOrderTask(self.values, name='task2',
                                provides='b', requires=['a']),
            utils.FailingTask(self.values, name='task3',
                              provides='c', requires=['b']),
            utils.SaveOrderTask(self.values, name='task1', provides='a'))

        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            engine.run()
        self.assertEqual(
            self.values,
            ['task1', 'task2',
             'task3 reverted(Failure: RuntimeError: Woot!)',
             'task2 reverted(5)', 'task1 reverted(5)'])
        self.assertEqual(engine.storage.get_flow_state(), states.REVERTED)

    def test_graph_flow_four_tasks_revert_failure(self):
        flow = gf.Flow('g-3-nasty').add(
            utils.NastyTask(name='task2', provides='b', requires=['a']),
            utils.FailingTask(self.values, name='task3', requires=['b']),
            utils.SaveOrderTask(self.values, name='task1', provides='a'))

        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Gotcha'):
            engine.run()
        self.assertEqual(engine.storage.get_flow_state(), states.FAILURE)

    def test_graph_flow_with_multireturn_and_multiargs_tasks(self):
        flow = gf.Flow('g-3-multi').add(
            utils.TaskMultiArgOneReturn(name='task1',
                                        rebind=['a', 'b', 'y'], provides='z'),
            utils.TaskMultiReturn(name='task2', provides=['a', 'b', 'c']),
            utils.TaskMultiArgOneReturn(name='task3',
                                        rebind=['c', 'b', 'x'], provides='y'))

        engine = self._make_engine(flow)
        engine.storage.inject({'x': 30})
        engine.run()
        self.assertEqual(engine.storage.fetch_all(), {
            'a': 1,
            'b': 3,
            'c': 5,
            'x': 30,
            'y': 38,
            'z': 42
        })

    def test_task_graph_property(self):
        flow = gf.Flow('test').add(
            utils.TaskNoRequiresNoReturns(name='task1'),
            utils.TaskNoRequiresNoReturns(name='task2'))

        engine = self._make_engine(flow)
        graph = engine.execution_graph
        self.assertTrue(isinstance(graph, networkx.DiGraph))

    def test_task_graph_property_for_one_task(self):
        flow = utils.TaskNoRequiresNoReturns(name='task1')

        engine = self._make_engine(flow)
        graph = engine.execution_graph
        self.assertTrue(isinstance(graph, networkx.DiGraph))


class SingleThreadedEngineTest(EngineTaskTest,
                               EngineLinearFlowTest,
                               EngineParallelFlowTest,
                               EngineGraphFlowTest,
                               test.TestCase):
    def _make_engine(self, flow, flow_detail=None):
        return taskflow.engines.load(flow,
                                     flow_detail=flow_detail,
                                     engine_conf='serial',
                                     backend=self.backend)

    def test_correct_load(self):
        engine = self._make_engine(utils.TaskNoRequiresNoReturns)
        self.assertIsInstance(engine, eng.SingleThreadedActionEngine)

    def test_singlethreaded_is_the_default(self):
        engine = taskflow.engines.load(utils.TaskNoRequiresNoReturns)
        self.assertIsInstance(engine, eng.SingleThreadedActionEngine)


class MultiThreadedEngineTest(EngineTaskTest,
                              EngineLinearFlowTest,
                              EngineParallelFlowTest,
                              EngineGraphFlowTest,
                              test.TestCase):
    def _make_engine(self, flow, flow_detail=None, executor=None):
        engine_conf = dict(engine='parallel',
                           executor=executor)
        return taskflow.engines.load(flow, flow_detail=flow_detail,
                                     engine_conf=engine_conf,
                                     backend=self.backend)

    def test_correct_load(self):
        engine = self._make_engine(utils.TaskNoRequiresNoReturns)
        self.assertIsInstance(engine, eng.MultiThreadedActionEngine)
        self.assertIs(engine.executor, None)

    def test_using_common_executor(self):
        flow = utils.TaskNoRequiresNoReturns(name='task1')
        executor = futures.ThreadPoolExecutor(2)
        try:
            e1 = self._make_engine(flow, executor=executor)
            e2 = self._make_engine(flow, executor=executor)
            self.assertIs(e1.executor, e2.executor)
        finally:
            executor.shutdown(wait=True)
