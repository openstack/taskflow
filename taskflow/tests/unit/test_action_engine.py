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
import time

from concurrent import futures

from taskflow.patterns import graph_flow as gf
from taskflow.patterns import linear_flow as lf
from taskflow.patterns import unordered_flow as uf

from taskflow.engines.action_engine import engine as eng
from taskflow import exceptions
from taskflow.persistence.backends import impl_memory
from taskflow.persistence import logbook
from taskflow import states
from taskflow import task
from taskflow import test
from taskflow.utils import persistence_utils as p_utils


class TestTask(task.Task):

    def __init__(self, values=None, name=None, sleep=None,
                 provides=None, rebind=None, requires=None):
        super(TestTask, self).__init__(name=name, provides=provides,
                                       rebind=rebind, requires=requires)
        if values is None:
            self.values = []
        else:
            self.values = values
        self._sleep = sleep

    def execute(self, **kwargs):
        self.update_progress(0.0)
        if self._sleep:
            time.sleep(self._sleep)
        self.values.append(self.name)
        self.update_progress(1.0)
        return 5

    def revert(self, **kwargs):
        self.update_progress(0)
        if self._sleep:
            time.sleep(self._sleep)
        self.values.append(self.name + ' reverted(%s)'
                           % kwargs.get('result'))
        self.update_progress(1.0)


class FailingTask(TestTask):

    def execute(self, **kwargs):
        self.update_progress(0)
        if self._sleep:
            time.sleep(self._sleep)
        self.update_progress(0.99)
        raise RuntimeError('Woot!')


class NeverRunningTask(task.Task):
    def execute(self, **kwargs):
        assert False, 'This method should not be called'

    def revert(self, **kwargs):
        assert False, 'This method should not be called'


class NastyTask(task.Task):
    def execute(self, **kwargs):
        pass

    def revert(self, **kwargs):
        raise RuntimeError('Gotcha!')


class MultiReturnTask(task.Task):
    def execute(self, **kwargs):
        return 12, 2, 1


class MultiargsTask(task.Task):
    def execute(self, a, b, c):
        return a + b + c


class MultiDictTask(task.Task):
    def execute(self):
        self.update_progress(0)
        output = {}
        total = len(sorted(self.provides))
        for i, k in enumerate(sorted(self.provides)):
            output[k] = i
            self.update_progress(i / total)
        self.update_progress(1.0)
        return output


class EngineTestBase(object):
    def setUp(self):
        super(EngineTestBase, self).setUp()
        self.values = []
        self.backend = impl_memory.MemoryBackend(conf={})
        self.book = p_utils.temporary_log_book(self.backend)

    def tearDown(self):
        super(EngineTestBase, self).tearDown()
        with contextlib.closing(self.backend) as be:
            with contextlib.closing(be.get_connection()) as conn:
                conn.clear_all()
        self.book = None

    def _make_engine(self, flow, flow_detail=None):
        raise NotImplementedError()


class EngineTaskTest(EngineTestBase):

    def test_run_task_as_flow(self):
        flow = lf.Flow('test-1')
        flow.add(TestTask(self.values, name='task1'))
        engine = self._make_engine(flow)
        engine.run()
        self.assertEquals(self.values, ['task1'])

    @staticmethod
    def _callback(state, values, details):
        name = details.get('task_name', '<unknown>')
        values.append('%s %s' % (name, state))

    @staticmethod
    def _flow_callback(state, values, details):
        values.append('flow %s' % state)

    def test_run_task_with_notifications(self):
        flow = TestTask(self.values, name='task1')
        engine = self._make_engine(flow)
        engine.notifier.register('*', self._flow_callback,
                                 kwargs={'values': self.values})
        engine.task_notifier.register('*', self._callback,
                                      kwargs={'values': self.values})
        engine.run()
        self.assertEquals(self.values,
                          ['flow RUNNING',
                           'task1 RUNNING',
                           'task1',
                           'task1 SUCCESS',
                           'flow SUCCESS'])

    def test_failing_task_with_notifications(self):
        flow = FailingTask(self.values, 'fail')
        engine = self._make_engine(flow)
        engine.notifier.register('*', self._flow_callback,
                                 kwargs={'values': self.values})
        engine.task_notifier.register('*', self._callback,
                                      kwargs={'values': self.values})
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            engine.run()
        self.assertEquals(self.values,
                          ['flow RUNNING',
                           'fail RUNNING',
                           'fail FAILURE',
                           'flow REVERTING',
                           'fail REVERTING',
                           'fail reverted(Failure: RuntimeError: Woot!)',
                           'fail REVERTED',
                           'fail PENDING',
                           'flow REVERTED',
                           'flow FAILURE'])

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

    def test_save_as(self):
        flow = TestTask(self.values, name='task1', provides='first_data')
        engine = self._make_engine(flow)
        engine.run()
        self.assertEquals(self.values, ['task1'])
        self.assertEquals(engine.storage.fetch_all(), {'first_data': 5})

    def test_save_all_in_one(self):
        flow = MultiReturnTask(provides='all_data')
        engine = self._make_engine(flow)
        engine.run()
        self.assertEquals(engine.storage.fetch_all(),
                          {'all_data': (12, 2, 1)})

    def test_save_several_values(self):
        flow = MultiReturnTask(provides=('badger', 'mushroom', 'snake'))
        engine = self._make_engine(flow)
        engine.run()
        self.assertEquals(engine.storage.fetch_all(), {
            'badger': 12,
            'mushroom': 2,
            'snake': 1
        })

    def test_save_dict(self):
        flow = MultiDictTask(provides=set(['badger', 'mushroom', 'snake']))
        engine = self._make_engine(flow)
        engine.run()
        self.assertEquals(engine.storage.fetch_all(), {
            'badger': 0,
            'mushroom': 1,
            'snake': 2,
        })

    def test_bad_save_as_value(self):
        with self.assertRaises(TypeError):
            TestTask(name='task1', provides=object())

    def test_arguments_passing(self):
        flow = MultiargsTask(provides='result')
        engine = self._make_engine(flow)
        engine.storage.inject({'a': 1, 'b': 4, 'c': 9, 'x': 17})
        engine.run()
        self.assertEquals(engine.storage.fetch_all(), {
            'a': 1, 'b': 4, 'c': 9, 'x': 17,
            'result': 14,
        })

    def test_arguments_missing(self):
        flow = MultiargsTask(provides='result')
        engine = self._make_engine(flow)
        engine.storage.inject({'a': 1, 'b': 4, 'x': 17})
        with self.assertRaises(exceptions.MissingDependencies):
            engine.run()

    def test_partial_arguments_mapping(self):
        flow = MultiargsTask(name='task1',
                             provides='result',
                             rebind={'b': 'x'})
        engine = self._make_engine(flow)
        engine.storage.inject({'a': 1, 'b': 4, 'c': 9, 'x': 17})
        engine.run()
        self.assertEquals(engine.storage.fetch_all(), {
            'a': 1, 'b': 4, 'c': 9, 'x': 17,
            'result': 27,
        })

    def test_all_arguments_mapping(self):
        flow = MultiargsTask(name='task1',
                             provides='result',
                             rebind=['x', 'y', 'z'])
        engine = self._make_engine(flow)
        engine.storage.inject({
            'a': 1, 'b': 2, 'c': 3, 'x': 4, 'y': 5, 'z': 6
        })
        engine.run()
        self.assertEquals(engine.storage.fetch_all(), {
            'a': 1, 'b': 2, 'c': 3, 'x': 4, 'y': 5, 'z': 6,
            'result': 15,
        })

    def test_invalid_argument_name_map(self):
        flow = MultiargsTask(name='task1', provides='result',
                             rebind={'b': 'z'})
        engine = self._make_engine(flow)
        engine.storage.inject({'a': 1, 'b': 4, 'c': 9, 'x': 17})
        with self.assertRaises(exceptions.MissingDependencies):
            engine.run()

    def test_invalid_argument_name_list(self):
        flow = MultiargsTask(name='task1',
                             provides='result',
                             rebind=['a', 'z', 'b'])
        engine = self._make_engine(flow)
        engine.storage.inject({'a': 1, 'b': 4, 'c': 9, 'x': 17})
        with self.assertRaises(exceptions.MissingDependencies):
            engine.run()

    def test_bad_rebind_args_value(self):
        with self.assertRaises(TypeError):
            TestTask(name='task1',
                     rebind=object())


class EngineLinearFlowTest(EngineTestBase):

    def test_sequential_flow_one_task(self):
        flow = lf.Flow('flow-1').add(
            TestTask(self.values, name='task1')
        )
        self._make_engine(flow).run()
        self.assertEquals(self.values, ['task1'])

    def test_sequential_flow_two_tasks(self):
        flow = lf.Flow('flow-2').add(
            TestTask(self.values, name='task1'),
            TestTask(self.values, name='task2')
        )
        self._make_engine(flow).run()
        self.assertEquals(self.values, ['task1', 'task2'])

    def test_revert_removes_data(self):
        flow = lf.Flow('revert-removes').add(
            TestTask(provides='one'),
            MultiReturnTask(provides=('a', 'b', 'c')),
            FailingTask(name='fail')
        )
        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            engine.run()
        self.assertEquals(engine.storage.fetch_all(), {})

    def test_sequential_flow_nested_blocks(self):
        flow = lf.Flow('nested-1').add(
            TestTask(self.values, 'task1'),
            lf.Flow('inner-1').add(
                TestTask(self.values, 'task2')
            )
        )
        self._make_engine(flow).run()
        self.assertEquals(self.values, ['task1', 'task2'])

    def test_revert_exception_is_reraised(self):
        flow = lf.Flow('revert-1').add(
            NastyTask(),
            FailingTask(name='fail')
        )
        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Gotcha'):
            engine.run()

    def test_revert_not_run_task_is_not_reverted(self):
        flow = lf.Flow('revert-not-run').add(
            FailingTask(self.values, 'fail'),
            NeverRunningTask(),
        )
        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            engine.run()
        self.assertEquals(self.values,
                          ['fail reverted(Failure: RuntimeError: Woot!)'])

    def test_correctly_reverts_children(self):
        flow = lf.Flow('root-1').add(
            TestTask(self.values, 'task1'),
            lf.Flow('child-1').add(
                TestTask(self.values, 'task2'),
                FailingTask(self.values, 'fail')
            )
        )
        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            engine.run()
        self.assertEquals(self.values,
                          ['task1', 'task2',
                           'fail reverted(Failure: RuntimeError: Woot!)',
                           'task2 reverted(5)', 'task1 reverted(5)'])


class EngineParallelFlowTest(EngineTestBase):

    def test_parallel_flow_one_task(self):
        flow = uf.Flow('p-1').add(
            TestTask(self.values, name='task1', sleep=0.01)
        )
        self._make_engine(flow).run()
        self.assertEquals(self.values, ['task1'])

    def test_parallel_flow_two_tasks(self):
        flow = uf.Flow('p-2').add(
            TestTask(self.values, name='task1', sleep=0.01),
            TestTask(self.values, name='task2', sleep=0.01)
        )
        self._make_engine(flow).run()

        result = set(self.values)
        self.assertEquals(result, set(['task1', 'task2']))

    def test_parallel_revert_common(self):
        flow = uf.Flow('p-r-3').add(
            TestTask(self.values, name='task1'),
            FailingTask(self.values, sleep=0.01),
            TestTask(self.values, name='task2')
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
                TestTask(self.values, name='task1'),
                NastyTask()
            ),
            FailingTask(self.values, sleep=0.1)
        )
        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Gotcha'):
            engine.run()

    def test_sequential_flow_two_tasks_with_resumption(self):
        flow = lf.Flow('lf-2-r').add(
            TestTask(self.values, name='task1', provides='x1'),
            TestTask(self.values, name='task2', provides='x2')
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
        self.assertEquals(self.values, ['task2'])
        self.assertEquals(engine.storage.fetch_all(),
                          {'x1': 17, 'x2': 5})


class EngineGraphFlowTest(EngineTestBase):

    def test_graph_flow_one_task(self):
        flow = gf.Flow('g-1').add(
            TestTask(self.values, name='task1')
        )
        self._make_engine(flow).run()
        self.assertEquals(self.values, ['task1'])

    def test_graph_flow_two_independent_tasks(self):
        flow = gf.Flow('g-2').add(
            TestTask(self.values, name='task1'),
            TestTask(self.values, name='task2')
        )
        self._make_engine(flow).run()
        self.assertEquals(set(self.values), set(['task1', 'task2']))

    def test_graph_flow_two_tasks(self):
        flow = gf.Flow('g-1-1').add(
            TestTask(self.values, name='task2', requires=['a']),
            TestTask(self.values, name='task1', provides='a')
        )
        self._make_engine(flow).run()
        self.assertEquals(self.values, ['task1', 'task2'])

    def test_graph_flow_four_tasks_added_separately(self):
        flow = (gf.Flow('g-4')
                .add(TestTask(self.values, name='task4',
                              provides='d', requires=['c']))
                .add(TestTask(self.values, name='task2',
                              provides='b', requires=['a']))
                .add(TestTask(self.values, name='task3',
                              provides='c', requires=['b']))
                .add(TestTask(self.values, name='task1',
                              provides='a'))
                )
        self._make_engine(flow).run()
        self.assertEquals(self.values, ['task1', 'task2', 'task3', 'task4'])

    def test_graph_cyclic_dependency(self):
        with self.assertRaisesRegexp(exceptions.DependencyFailure, '^No path'):
            gf.Flow('g-3-cyclic').add(
                TestTask([], name='task1', provides='a', requires=['b']),
                TestTask([], name='task2', provides='b', requires=['c']),
                TestTask([], name='task3', provides='c', requires=['a']))

    def test_graph_two_tasks_returns_same_value(self):
        with self.assertRaisesRegexp(exceptions.DependencyFailure,
                                     "task2 provides a but is already being"
                                     " provided by task1 and duplicate"
                                     " producers are disallowed"):
            gf.Flow('g-2-same-value').add(
                TestTask([], name='task1', provides='a'),
                TestTask([], name='task2', provides='a'))

    def test_graph_flow_four_tasks_revert(self):
        flow = gf.Flow('g-4-failing').add(
            TestTask(self.values, name='task4', provides='d', requires=['c']),
            TestTask(self.values, name='task2', provides='b', requires=['a']),
            FailingTask(self.values, name='task3',
                        provides='c', requires=['b']),
            TestTask(self.values, name='task1', provides='a'))

        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            engine.run()
        self.assertEquals(self.values,
                          ['task1', 'task2',
                           'task3 reverted(Failure: RuntimeError: Woot!)',
                           'task2 reverted(5)', 'task1 reverted(5)'])

    def test_graph_flow_four_tasks_revert_failure(self):
        flow = gf.Flow('g-3-nasty').add(
            NastyTask(name='task2', provides='b', requires=['a']),
            FailingTask(self.values, name='task3', requires=['b']),
            TestTask(self.values, name='task1', provides='a'))

        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Gotcha'):
            engine.run()

    def test_graph_flow_with_multireturn_and_multiargs_tasks(self):
        flow = gf.Flow('g-3-multi').add(
            MultiargsTask(name='task1', rebind=['a', 'b', 'y'], provides='z'),
            MultiReturnTask(name='task2', provides=['a', 'b', 'c']),
            MultiargsTask(name='task3', rebind=['c', 'b', 'x'], provides='y'))

        engine = self._make_engine(flow)
        engine.storage.inject({'x': 30})
        engine.run()
        self.assertEquals(engine.storage.fetch_all(), {
            'a': 12,
            'b': 2,
            'c': 1,
            'x': 30,
            'y': 33,
            'z': 47
        })

    def test_one_task_provides_and_requires_same_data(self):
        with self.assertRaisesRegexp(exceptions.DependencyFailure, '^No path'):
            gf.Flow('g-1-req-error').add(
                TestTask([], name='task1', requires=['a'], provides='a'))

    def test_task_graph_property(self):
        flow = gf.Flow('test').add(
            TestTask(name='task1'),
            TestTask(name='task2'))

        engine = self._make_engine(flow)
        graph = engine.get_graph()
        self.assertTrue(isinstance(graph, networkx.DiGraph))

    def test_task_graph_property_for_one_task(self):
        flow = TestTask(name='task1')

        engine = self._make_engine(flow)
        graph = engine.get_graph()
        self.assertTrue(isinstance(graph, networkx.DiGraph))


class SingleThreadedEngineTest(EngineTaskTest,
                               EngineLinearFlowTest,
                               EngineParallelFlowTest,
                               EngineGraphFlowTest,
                               test.TestCase):
    def _make_engine(self, flow, flow_detail=None):
        if flow_detail is None:
            flow_detail = p_utils.create_flow_detail(flow, self.book,
                                                     self.backend)
        return eng.SingleThreadedActionEngine(flow, backend=self.backend,
                                              flow_detail=flow_detail)


class MultiThreadedEngineTest(EngineTaskTest,
                              EngineLinearFlowTest,
                              EngineParallelFlowTest,
                              EngineGraphFlowTest,
                              test.TestCase):
    def _make_engine(self, flow, flow_detail=None, executor=None):
        if flow_detail is None:
            flow_detail = p_utils.create_flow_detail(flow, self.book,
                                                     self.backend)
        return eng.MultiThreadedActionEngine(flow, backend=self.backend,
                                             flow_detail=flow_detail,
                                             executor=executor)

    def test_using_common_pool(self):
        flow = TestTask(self.values, name='task1')
        executor = futures.ThreadPoolExecutor(2)
        e1 = self._make_engine(flow, executor=executor)
        e2 = self._make_engine(flow, executor=executor)
        self.assertIs(e1.executor, e2.executor)

    def test_parallel_revert_specific(self):
        flow = uf.Flow('p-r-r').add(
            TestTask(self.values, name='task1', sleep=0.01),
            FailingTask(sleep=0.01),
            TestTask(self.values, name='task2', sleep=0.01)
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
            TestTask(self.values, name='task1', sleep=0.01),
            NastyTask(),
            FailingTask(sleep=0.01),
            TestTask()  # this should not get reverted
        )
        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Gotcha'):
            engine.run()
        result = set(self.values)
        self.assertEquals(result, set(['task1']))

    def test_nested_parallel_revert_exception_is_reraised(self):
        flow = uf.Flow('p-root').add(
            TestTask(self.values, name='task1'),
            TestTask(self.values, name='task2'),
            lf.Flow('p-inner').add(
                TestTask(self.values, name='task3', sleep=0.1),
                NastyTask(),
                FailingTask(sleep=0.01)
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
            TestTask(self.values, name='task1'),
            TestTask(self.values, name='task2'),
            uf.Flow('p-inner').add(
                TestTask(self.values, name='task3', sleep=0.1),
                NastyTask(),
                FailingTask(sleep=0.01)
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
            TestTask(self.values, name='task1'),
            TestTask(self.values, name='task2'),
            uf.Flow('p-inner').add(
                TestTask(self.values, name='task3', sleep=0.1),
                FailingTask(sleep=0.01)
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
            TestTask(self.values, name='task1'),
            TestTask(self.values, name='task2'),
            lf.Flow('l-inner').add(
                TestTask(self.values, name='task3', sleep=0.1),
                FailingTask(self.values, name='fail', sleep=0.01)
            )
        )
        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            engine.run()
        result = set(self.values)
        # Since this is an unordered flow we can not guarantee that task1 or
        # task2 will exist and be reverted, although they may exist depending
        # on how the OS thread scheduling and execution graph algorithm...
        possible_result = set(['task1', 'task1 reverted(5)',
                               'task2', 'task2 reverted(5)',
                               'task3', 'task3 reverted(5)',
                               'fail reverted(Failure: RuntimeError: Woot!)'])
        self.assertIsSubset(possible_result, result)

    def test_linear_nested_to_parallel_revert_exception(self):
        flow = uf.Flow('p-root').add(
            TestTask(self.values, name='task1', sleep=0.01),
            TestTask(self.values, name='task2', sleep=0.01),
            lf.Flow('l-inner').add(
                TestTask(self.values, name='task3'),
                NastyTask(),
                FailingTask(sleep=0.01)
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
