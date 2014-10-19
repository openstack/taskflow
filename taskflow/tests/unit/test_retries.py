# -*- coding: utf-8 -*-

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

import taskflow.engines
from taskflow import exceptions as exc
from taskflow.patterns import graph_flow as gf
from taskflow.patterns import linear_flow as lf
from taskflow.patterns import unordered_flow as uf
from taskflow import retry
from taskflow import states as st
from taskflow import test
from taskflow.tests import utils
from taskflow.types import failure


class RetryTest(utils.EngineTestBase):

    def test_run_empty_linear_flow(self):
        flow = lf.Flow('flow-1', utils.OneReturnRetry(provides='x'))
        engine = self._make_engine(flow)
        engine.run()
        self.assertEqual(engine.storage.fetch_all(), {'x': 1})

    def test_run_empty_unordered_flow(self):
        flow = uf.Flow('flow-1', utils.OneReturnRetry(provides='x'))
        engine = self._make_engine(flow)
        engine.run()
        self.assertEqual(engine.storage.fetch_all(), {'x': 1})

    def test_run_empty_graph_flow(self):
        flow = gf.Flow('flow-1', utils.OneReturnRetry(provides='x'))
        engine = self._make_engine(flow)
        engine.run()
        self.assertEqual(engine.storage.fetch_all(), {'x': 1})

    def test_states_retry_success_linear_flow(self):
        flow = lf.Flow('flow-1', retry.Times(4, 'r1', provides='x')).add(
            utils.SaveOrderTask("task1"),
            utils.ConditionalTask("task2")
        )
        engine = self._make_engine(flow)
        utils.register_notifiers(engine, self.values)
        engine.storage.inject({'y': 2})
        engine.run()
        self.assertEqual(engine.storage.fetch_all(), {'y': 2, 'x': 2})
        expected = ['flow RUNNING',
                    'r1 RUNNING',
                    'r1 SUCCESS',
                    'task1 RUNNING',
                    'task1',
                    'task1 SUCCESS',
                    'task2 RUNNING',
                    'task2',
                    'task2 FAILURE',
                    'task2 REVERTING',
                    u'task2 reverted(Failure: RuntimeError: Woot!)',
                    'task2 REVERTED',
                    'task1 REVERTING',
                    'task1 reverted(5)',
                    'task1 REVERTED',
                    'r1 RETRYING',
                    'task1 PENDING',
                    'task2 PENDING',
                    'r1 RUNNING',
                    'r1 SUCCESS',
                    'task1 RUNNING',
                    'task1',
                    'task1 SUCCESS',
                    'task2 RUNNING',
                    'task2',
                    'task2 SUCCESS',
                    'flow SUCCESS']
        self.assertEqual(self.values, expected)

    def test_states_retry_reverted_linear_flow(self):
        flow = lf.Flow('flow-1', retry.Times(2, 'r1', provides='x')).add(
            utils.SaveOrderTask("task1"),
            utils.ConditionalTask("task2")
        )
        engine = self._make_engine(flow)
        utils.register_notifiers(engine, self.values)
        engine.storage.inject({'y': 4})
        self.assertRaisesRegexp(RuntimeError, '^Woot', engine.run)
        self.assertEqual(engine.storage.fetch_all(), {'y': 4})
        expected = ['flow RUNNING',
                    'r1 RUNNING',
                    'r1 SUCCESS',
                    'task1 RUNNING',
                    'task1',
                    'task1 SUCCESS',
                    'task2 RUNNING',
                    'task2',
                    'task2 FAILURE',
                    'task2 REVERTING',
                    u'task2 reverted(Failure: RuntimeError: Woot!)',
                    'task2 REVERTED',
                    'task1 REVERTING',
                    'task1 reverted(5)',
                    'task1 REVERTED',
                    'r1 RETRYING',
                    'task1 PENDING',
                    'task2 PENDING',
                    'r1 RUNNING',
                    'r1 SUCCESS',
                    'task1 RUNNING',
                    'task1',
                    'task1 SUCCESS',
                    'task2 RUNNING',
                    'task2',
                    'task2 FAILURE',
                    'task2 REVERTING',
                    u'task2 reverted(Failure: RuntimeError: Woot!)',
                    'task2 REVERTED',
                    'task1 REVERTING',
                    'task1 reverted(5)',
                    'task1 REVERTED',
                    'r1 REVERTING',
                    'r1 REVERTED',
                    'flow REVERTED']
        self.assertEqual(self.values, expected)

    def test_states_retry_failure_linear_flow(self):
        flow = lf.Flow('flow-1', retry.Times(2, 'r1', provides='x')).add(
            utils.NastyTask("task1"),
            utils.ConditionalTask("task2")
        )
        engine = self._make_engine(flow)
        utils.register_notifiers(engine, self.values)
        engine.storage.inject({'y': 4})
        self.assertRaisesRegexp(RuntimeError, '^Gotcha', engine.run)
        self.assertEqual(engine.storage.fetch_all(), {'y': 4, 'x': 1})
        expected = ['flow RUNNING',
                    'r1 RUNNING',
                    'r1 SUCCESS',
                    'task1 RUNNING',
                    'task1 SUCCESS',
                    'task2 RUNNING',
                    'task2',
                    'task2 FAILURE',
                    'task2 REVERTING',
                    u'task2 reverted(Failure: RuntimeError: Woot!)',
                    'task2 REVERTED',
                    'task1 REVERTING',
                    'task1 FAILURE',
                    'flow FAILURE']
        self.assertEqual(self.values, expected)

    def test_states_retry_failure_nested_flow_fails(self):
        flow = lf.Flow('flow-1', utils.retry.AlwaysRevert('r1')).add(
            utils.TaskNoRequiresNoReturns("task1"),
            lf.Flow('flow-2', retry.Times(3, 'r2', provides='x')).add(
                utils.TaskNoRequiresNoReturns("task2"),
                utils.ConditionalTask("task3")
            ),
            utils.TaskNoRequiresNoReturns("task4")
        )
        engine = self._make_engine(flow)
        utils.register_notifiers(engine, self.values)
        engine.storage.inject({'y': 2})
        engine.run()
        self.assertEqual(engine.storage.fetch_all(), {'y': 2, 'x': 2})
        expected = ['flow RUNNING',
                    'r1 RUNNING',
                    'r1 SUCCESS',
                    'task1 RUNNING',
                    'task1 SUCCESS',
                    'r2 RUNNING',
                    'r2 SUCCESS',
                    'task2 RUNNING',
                    'task2 SUCCESS',
                    'task3 RUNNING',
                    'task3',
                    'task3 FAILURE',
                    'task3 REVERTING',
                    u'task3 reverted(Failure: RuntimeError: Woot!)',
                    'task3 REVERTED',
                    'task2 REVERTING',
                    'task2 REVERTED',
                    'r2 RETRYING',
                    'task2 PENDING',
                    'task3 PENDING',
                    'r2 RUNNING',
                    'r2 SUCCESS',
                    'task2 RUNNING',
                    'task2 SUCCESS',
                    'task3 RUNNING',
                    'task3',
                    'task3 SUCCESS',
                    'task4 RUNNING',
                    'task4 SUCCESS',
                    'flow SUCCESS']
        self.assertEqual(self.values, expected)

    def test_states_retry_failure_parent_flow_fails(self):
        flow = lf.Flow('flow-1', retry.Times(3, 'r1', provides='x1')).add(
            utils.TaskNoRequiresNoReturns("task1"),
            lf.Flow('flow-2', retry.Times(3, 'r2', provides='x2')).add(
                utils.TaskNoRequiresNoReturns("task2"),
                utils.TaskNoRequiresNoReturns("task3")
            ),
            utils.ConditionalTask("task4", rebind={'x': 'x1'})
        )
        engine = self._make_engine(flow)
        utils.register_notifiers(engine, self.values)
        engine.storage.inject({'y': 2})
        engine.run()
        self.assertEqual(engine.storage.fetch_all(), {'y': 2, 'x1': 2,
                                                      'x2': 1})
        expected = ['flow RUNNING',
                    'r1 RUNNING',
                    'r1 SUCCESS',
                    'task1 RUNNING',
                    'task1 SUCCESS',
                    'r2 RUNNING',
                    'r2 SUCCESS',
                    'task2 RUNNING',
                    'task2 SUCCESS',
                    'task3 RUNNING',
                    'task3 SUCCESS',
                    'task4 RUNNING',
                    'task4',
                    'task4 FAILURE',
                    'task4 REVERTING',
                    u'task4 reverted(Failure: RuntimeError: Woot!)',
                    'task4 REVERTED',
                    'task3 REVERTING',
                    'task3 REVERTED',
                    'task2 REVERTING',
                    'task2 REVERTED',
                    'r2 REVERTING',
                    'r2 REVERTED',
                    'task1 REVERTING',
                    'task1 REVERTED',
                    'r1 RETRYING',
                    'task1 PENDING',
                    'r2 PENDING',
                    'task2 PENDING',
                    'task3 PENDING',
                    'task4 PENDING',
                    'r1 RUNNING',
                    'r1 SUCCESS',
                    'task1 RUNNING',
                    'task1 SUCCESS',
                    'r2 RUNNING',
                    'r2 SUCCESS',
                    'task2 RUNNING',
                    'task2 SUCCESS',
                    'task3 RUNNING',
                    'task3 SUCCESS',
                    'task4 RUNNING',
                    'task4',
                    'task4 SUCCESS',
                    'flow SUCCESS']
        self.assertEqual(self.values, expected)

    def test_unordered_flow_task_fails_parallel_tasks_should_be_reverted(self):
        flow = uf.Flow('flow-1', retry.Times(3, 'r', provides='x')).add(
            utils.SaveOrderTask("task1"),
            utils.ConditionalTask("task2")
        )
        engine = self._make_engine(flow)
        engine.storage.inject({'y': 2})
        engine.run()
        self.assertEqual(engine.storage.fetch_all(), {'y': 2, 'x': 2})
        expected = ['task2',
                    'task1',
                    u'task2 reverted(Failure: RuntimeError: Woot!)',
                    'task1 reverted(5)',
                    'task2',
                    'task1']
        self.assertItemsEqual(self.values, expected)

    def test_nested_flow_reverts_parent_retries(self):
        retry1 = retry.Times(3, 'r1', provides='x')
        retry2 = retry.Times(0, 'r2', provides='x2')

        flow = lf.Flow('flow-1', retry1).add(
            utils.SaveOrderTask("task1"),
            lf.Flow('flow-2', retry2).add(utils.ConditionalTask("task2"))
        )
        engine = self._make_engine(flow)
        engine.storage.inject({'y': 2})
        utils.register_notifiers(engine, self.values)
        engine.run()
        self.assertEqual(engine.storage.fetch_all(), {'y': 2, 'x': 2, 'x2': 1})
        expected = ['flow RUNNING',
                    'r1 RUNNING',
                    'r1 SUCCESS',
                    'task1 RUNNING',
                    'task1',
                    'task1 SUCCESS',
                    'r2 RUNNING',
                    'r2 SUCCESS',
                    'task2 RUNNING',
                    'task2',
                    'task2 FAILURE',
                    'task2 REVERTING',
                    u'task2 reverted(Failure: RuntimeError: Woot!)',
                    'task2 REVERTED',
                    'r2 REVERTING',
                    'r2 REVERTED',
                    'task1 REVERTING',
                    'task1 reverted(5)',
                    'task1 REVERTED',
                    'r1 RETRYING',
                    'task1 PENDING',
                    'r2 PENDING',
                    'task2 PENDING',
                    'r1 RUNNING',
                    'r1 SUCCESS',
                    'task1 RUNNING',
                    'task1',
                    'task1 SUCCESS',
                    'r2 RUNNING',
                    'r2 SUCCESS',
                    'task2 RUNNING',
                    'task2',
                    'task2 SUCCESS',
                    'flow SUCCESS']
        self.assertEqual(self.values, expected)

    def test_revert_all_retry(self):
        flow = lf.Flow('flow-1', retry.Times(3, 'r1', provides='x')).add(
            utils.SaveOrderTask("task1"),
            lf.Flow('flow-2', retry.AlwaysRevertAll('r2')).add(
                utils.ConditionalTask("task2"))
        )
        engine = self._make_engine(flow)
        engine.storage.inject({'y': 2})
        utils.register_notifiers(engine, self.values)
        self.assertRaisesRegexp(RuntimeError, '^Woot', engine.run)
        self.assertEqual(engine.storage.fetch_all(), {'y': 2})
        expected = ['flow RUNNING',
                    'r1 RUNNING',
                    'r1 SUCCESS',
                    'task1 RUNNING',
                    'task1',
                    'task1 SUCCESS',
                    'r2 RUNNING',
                    'r2 SUCCESS',
                    'task2 RUNNING',
                    'task2',
                    'task2 FAILURE',
                    'task2 REVERTING',
                    u'task2 reverted(Failure: RuntimeError: Woot!)',
                    'task2 REVERTED',
                    'r2 REVERTING',
                    'r2 REVERTED',
                    'task1 REVERTING',
                    'task1 reverted(5)',
                    'task1 REVERTED',
                    'r1 REVERTING',
                    'r1 REVERTED',
                    'flow REVERTED']
        self.assertEqual(self.values, expected)

    def test_restart_reverted_flow_with_retry(self):
        flow = lf.Flow('test', retry=utils.OneReturnRetry(provides='x')).add(
            utils.FailingTask('fail'))
        engine = self._make_engine(flow)
        self.assertRaisesRegexp(RuntimeError, '^Woot', engine.run)
        self.assertRaisesRegexp(RuntimeError, '^Woot', engine.run)

    def test_run_just_retry(self):
        flow = utils.OneReturnRetry(provides='x')
        engine = self._make_engine(flow)
        self.assertRaisesRegexp(TypeError, 'Retry controller', engine.run)

    def test_use_retry_as_a_task(self):
        flow = lf.Flow('test').add(utils.OneReturnRetry(provides='x'))
        engine = self._make_engine(flow)
        self.assertRaisesRegexp(TypeError, 'Retry controller', engine.run)

    def test_resume_flow_that_had_been_interrupted_during_retrying(self):
        flow = lf.Flow('flow-1', retry.Times(3, 'r1')).add(
            utils.SaveOrderTask('t1'),
            utils.SaveOrderTask('t2'),
            utils.SaveOrderTask('t3')
        )
        engine = self._make_engine(flow)
        engine.compile()
        engine.prepare()
        utils.register_notifiers(engine, self.values)
        engine.storage.set_atom_state('r1', st.RETRYING)
        engine.storage.set_atom_state('t1', st.PENDING)
        engine.storage.set_atom_state('t2', st.REVERTED)
        engine.storage.set_atom_state('t3', st.REVERTED)

        engine.run()
        expected = ['flow RUNNING',
                    't2 PENDING',
                    't3 PENDING',
                    'r1 RUNNING',
                    'r1 SUCCESS',
                    't1 RUNNING',
                    't1',
                    't1 SUCCESS',
                    't2 RUNNING',
                    't2',
                    't2 SUCCESS',
                    't3 RUNNING',
                    't3',
                    't3 SUCCESS',
                    'flow SUCCESS']
        self.assertEqual(self.values, expected)

    def test_resume_flow_that_should_be_retried(self):
        flow = lf.Flow('flow-1', retry.Times(3, 'r1')).add(
            utils.SaveOrderTask('t1'),
            utils.SaveOrderTask('t2')
        )
        engine = self._make_engine(flow)
        engine.compile()
        engine.prepare()
        utils.register_notifiers(engine, self.values)
        engine.storage.set_atom_intention('r1', st.RETRY)
        engine.storage.set_atom_state('r1', st.SUCCESS)
        engine.storage.set_atom_state('t1', st.REVERTED)
        engine.storage.set_atom_state('t2', st.REVERTED)

        engine.run()
        expected = ['flow RUNNING',
                    'r1 RETRYING',
                    't1 PENDING',
                    't2 PENDING',
                    'r1 RUNNING',
                    'r1 SUCCESS',
                    't1 RUNNING',
                    't1',
                    't1 SUCCESS',
                    't2 RUNNING',
                    't2',
                    't2 SUCCESS',
                    'flow SUCCESS']
        self.assertEqual(self.values, expected)

    def test_retry_tasks_that_has_not_been_reverted(self):
        flow = lf.Flow('flow-1', retry.Times(3, 'r1', provides='x')).add(
            utils.ConditionalTask('c'),
            utils.SaveOrderTask('t1')
        )
        engine = self._make_engine(flow)
        engine.storage.inject({'y': 2})
        engine.run()
        expected = ['c',
                    u'c reverted(Failure: RuntimeError: Woot!)',
                    'c',
                    't1']
        self.assertEqual(self.values, expected)

    def test_default_times_retry(self):
        flow = lf.Flow('flow-1', retry.Times(3, 'r1')).add(
            utils.SaveOrderTask('t1'),
            utils.FailingTask('t2'))
        engine = self._make_engine(flow)

        self.assertRaisesRegexp(RuntimeError, '^Woot', engine.run)
        expected = ['t1',
                    u't2 reverted(Failure: RuntimeError: Woot!)',
                    't1 reverted(5)',
                    't1',
                    u't2 reverted(Failure: RuntimeError: Woot!)',
                    't1 reverted(5)',
                    't1',
                    u't2 reverted(Failure: RuntimeError: Woot!)',
                    't1 reverted(5)']
        self.assertEqual(self.values, expected)

    def test_for_each_with_list(self):
        collection = [3, 2, 3, 5]
        retry1 = retry.ForEach(collection, 'r1', provides='x')
        flow = lf.Flow('flow-1', retry1).add(utils.FailingTaskWithOneArg('t1'))
        engine = self._make_engine(flow)

        self.assertRaisesRegexp(RuntimeError, '^Woot', engine.run)
        expected = [u't1 reverted(Failure: RuntimeError: Woot with 3)',
                    u't1 reverted(Failure: RuntimeError: Woot with 2)',
                    u't1 reverted(Failure: RuntimeError: Woot with 3)',
                    u't1 reverted(Failure: RuntimeError: Woot with 5)']
        self.assertEqual(self.values, expected)

    def test_for_each_with_set(self):
        collection = set([3, 2, 5])
        retry1 = retry.ForEach(collection, 'r1', provides='x')
        flow = lf.Flow('flow-1', retry1).add(utils.FailingTaskWithOneArg('t1'))
        engine = self._make_engine(flow)

        self.assertRaisesRegexp(RuntimeError, '^Woot', engine.run)
        expected = [u't1 reverted(Failure: RuntimeError: Woot with 3)',
                    u't1 reverted(Failure: RuntimeError: Woot with 2)',
                    u't1 reverted(Failure: RuntimeError: Woot with 5)']
        self.assertItemsEqual(self.values, expected)

    def test_for_each_empty_collection(self):
        values = []
        retry1 = retry.ForEach(values, 'r1', provides='x')
        flow = lf.Flow('flow-1', retry1).add(utils.ConditionalTask('t1'))
        engine = self._make_engine(flow)
        engine.storage.inject({'y': 1})
        self.assertRaisesRegexp(exc.NotFound, '^No elements left', engine.run)

    def test_parameterized_for_each_with_list(self):
        values = [3, 2, 5]
        retry1 = retry.ParameterizedForEach('r1', provides='x')
        flow = lf.Flow('flow-1', retry1).add(utils.FailingTaskWithOneArg('t1'))
        engine = self._make_engine(flow)
        engine.storage.inject({'values': values, 'y': 1})

        self.assertRaisesRegexp(RuntimeError, '^Woot', engine.run)
        expected = [u't1 reverted(Failure: RuntimeError: Woot with 3)',
                    u't1 reverted(Failure: RuntimeError: Woot with 2)',
                    u't1 reverted(Failure: RuntimeError: Woot with 5)']
        self.assertEqual(self.values, expected)

    def test_parameterized_for_each_with_set(self):
        values = ([3, 2, 5])
        retry1 = retry.ParameterizedForEach('r1', provides='x')
        flow = lf.Flow('flow-1', retry1).add(utils.FailingTaskWithOneArg('t1'))
        engine = self._make_engine(flow)
        engine.storage.inject({'values': values, 'y': 1})

        self.assertRaisesRegexp(RuntimeError, '^Woot', engine.run)
        expected = [u't1 reverted(Failure: RuntimeError: Woot with 3)',
                    u't1 reverted(Failure: RuntimeError: Woot with 2)',
                    u't1 reverted(Failure: RuntimeError: Woot with 5)']
        self.assertItemsEqual(self.values, expected)

    def test_parameterized_for_each_empty_collection(self):
        values = []
        retry1 = retry.ParameterizedForEach('r1', provides='x')
        flow = lf.Flow('flow-1', retry1).add(utils.ConditionalTask('t1'))
        engine = self._make_engine(flow)
        engine.storage.inject({'values': values, 'y': 1})
        self.assertRaisesRegexp(exc.NotFound, '^No elements left', engine.run)

    def _pretend_to_run_a_flow_and_crash(self, when):
        flow = uf.Flow('flow-1', retry.Times(3, provides='x')).add(
            utils.SaveOrderTask('task1'))
        engine = self._make_engine(flow)
        engine.compile()
        engine.prepare()
        # imagine we run engine
        engine.storage.set_flow_state(st.RUNNING)
        engine.storage.set_atom_intention('flow-1_retry', st.EXECUTE)
        engine.storage.set_atom_intention('task1', st.EXECUTE)
        # we execute retry
        engine.storage.save('flow-1_retry', 1)
        # task fails
        fail = failure.Failure.from_exception(RuntimeError('foo')),
        engine.storage.save('task1', fail, state=st.FAILURE)
        if when == 'task fails':
            return engine
        # we save it's failure to retry and ask what to do
        engine.storage.save_retry_failure('flow-1_retry', 'task1', fail)
        if when == 'retry queried':
            return engine
        # it returned 'RETRY', so we update it's intention
        engine.storage.set_atom_intention('flow-1_retry', st.RETRY)
        if when == 'retry updated':
            return engine
        # we set task1 intention to REVERT
        engine.storage.set_atom_intention('task1', st.REVERT)
        if when == 'task updated':
            return engine
        # we schedule task1 for reversion
        engine.storage.set_atom_state('task1', st.REVERTING)
        if when == 'revert scheduled':
            return engine
        raise ValueError('Invalid crash point: %s' % when)

    def test_resumption_on_crash_after_task_failure(self):
        engine = self._pretend_to_run_a_flow_and_crash('task fails')
        # then process die and we resume engine
        engine.run()
        expected = [u'task1 reverted(Failure: RuntimeError: foo)', 'task1']
        self.assertEqual(self.values, expected)

    def test_resumption_on_crash_after_retry_queried(self):
        engine = self._pretend_to_run_a_flow_and_crash('retry queried')
        # then process die and we resume engine
        engine.run()
        expected = [u'task1 reverted(Failure: RuntimeError: foo)', 'task1']
        self.assertEqual(self.values, expected)

    def test_resumption_on_crash_after_retry_updated(self):
        engine = self._pretend_to_run_a_flow_and_crash('retry updated')
        # then process die and we resume engine
        engine.run()
        expected = [u'task1 reverted(Failure: RuntimeError: foo)', 'task1']
        self.assertEqual(self.values, expected)

    def test_resumption_on_crash_after_task_updated(self):
        engine = self._pretend_to_run_a_flow_and_crash('task updated')
        # then process die and we resume engine
        engine.run()
        expected = [u'task1 reverted(Failure: RuntimeError: foo)', 'task1']
        self.assertEqual(self.values, expected)

    def test_resumption_on_crash_after_revert_scheduled(self):
        engine = self._pretend_to_run_a_flow_and_crash('revert scheduled')
        # then process die and we resume engine
        engine.run()
        expected = [u'task1 reverted(Failure: RuntimeError: foo)', 'task1']
        self.assertEqual(self.values, expected)

    def test_retry_fails(self):

        class FailingRetry(retry.Retry):

            def execute(self, **kwargs):
                raise ValueError('OMG I FAILED')

            def revert(self, history, **kwargs):
                self.history = history

            def on_failure(self, **kwargs):
                return retry.REVERT

        r = FailingRetry()
        flow = lf.Flow('testflow', r)
        self.assertRaisesRegexp(ValueError, '^OMG',
                                self._make_engine(flow).run)
        self.assertEqual(len(r.history), 1)
        self.assertEqual(r.history[0][1], {})
        self.assertEqual(isinstance(r.history[0][0], failure.Failure), True)

    def test_retry_revert_fails(self):

        class FailingRetry(retry.Retry):

            def execute(self, **kwargs):
                raise ValueError('OMG I FAILED')

            def revert(self, history, **kwargs):
                raise ValueError('WOOT!')

            def on_failure(self, **kwargs):
                return retry.REVERT

        r = FailingRetry()
        flow = lf.Flow('testflow', r)
        engine = self._make_engine(flow)
        self.assertRaisesRegexp(ValueError, '^WOOT', engine.run)

    def test_nested_provides_graph_reverts_correctly(self):
        flow = gf.Flow("test").add(
            utils.SaveOrderTask('a', requires=['x']),
            lf.Flow("test2", retry=retry.Times(2)).add(
                utils.SaveOrderTask('b', provides='x'),
                utils.FailingTask('c')))
        engine = self._make_engine(flow)
        engine.compile()
        engine.prepare()
        engine.storage.save('test2_retry', 1)
        engine.storage.save('b', 11)
        engine.storage.save('a', 10)
        self.assertRaisesRegexp(RuntimeError, '^Woot', engine.run)
        self.assertItemsEqual(self.values[:3], [
            'a reverted(10)',
            'c reverted(Failure: RuntimeError: Woot!)',
            'b reverted(11)',
        ])
        # Task 'a' was or was not executed again, both cases are ok.
        self.assertIsSuperAndSubsequence(self.values[3:], [
            'b',
            'c reverted(Failure: RuntimeError: Woot!)',
            'b reverted(5)'
        ])
        self.assertEqual(engine.storage.get_flow_state(), st.REVERTED)

    def test_nested_provides_graph_retried_correctly(self):
        flow = gf.Flow("test").add(
            utils.SaveOrderTask('a', requires=['x']),
            lf.Flow("test2", retry=retry.Times(2)).add(
                utils.SaveOrderTask('b', provides='x'),
                utils.SaveOrderTask('c')))
        engine = self._make_engine(flow)
        engine.compile()
        engine.prepare()
        engine.storage.save('test2_retry', 1)
        engine.storage.save('b', 11)
        # pretend that 'c' failed
        fail = failure.Failure.from_exception(RuntimeError('Woot!'))
        engine.storage.save('c', fail, st.FAILURE)

        engine.run()
        self.assertItemsEqual(self.values[:2], [
            'c reverted(Failure: RuntimeError: Woot!)',
            'b reverted(11)',
        ])
        self.assertItemsEqual(self.values[2:], ['b', 'c', 'a'])
        self.assertEqual(engine.storage.get_flow_state(), st.SUCCESS)


class RetryParallelExecutionTest(utils.EngineTestBase):

    def test_when_subflow_fails_revert_running_tasks(self):
        waiting_task = utils.WaitForOneFromTask('task1', 'task2',
                                                [st.SUCCESS, st.FAILURE])
        flow = uf.Flow('flow-1', retry.Times(3, 'r', provides='x')).add(
            waiting_task,
            utils.ConditionalTask('task2')
        )
        engine = self._make_engine(flow)
        engine.task_notifier.register('*', waiting_task.callback)
        engine.storage.inject({'y': 2})
        engine.run()
        self.assertEqual(engine.storage.fetch_all(), {'y': 2, 'x': 2})
        expected = ['task2',
                    'task1',
                    u'task2 reverted(Failure: RuntimeError: Woot!)',
                    'task1 reverted(5)',
                    'task2',
                    'task1']
        self.assertItemsEqual(self.values, expected)

    def test_when_subflow_fails_revert_success_tasks(self):
        waiting_task = utils.WaitForOneFromTask('task2', 'task1',
                                                [st.SUCCESS, st.FAILURE])
        flow = uf.Flow('flow-1', retry.Times(3, 'r', provides='x')).add(
            utils.SaveOrderTask('task1'),
            lf.Flow('flow-2').add(
                waiting_task,
                utils.ConditionalTask('task3'))
        )
        engine = self._make_engine(flow)
        engine.task_notifier.register('*', waiting_task.callback)
        engine.storage.inject({'y': 2})
        engine.run()
        self.assertEqual(engine.storage.fetch_all(), {'y': 2, 'x': 2})
        expected = ['task1',
                    'task2',
                    'task3',
                    u'task3 reverted(Failure: RuntimeError: Woot!)',
                    'task1 reverted(5)',
                    'task2 reverted(5)',
                    'task1',
                    'task2',
                    'task3']
        self.assertItemsEqual(self.values, expected)


class SingleThreadedEngineTest(RetryTest,
                               test.TestCase):
    def _make_engine(self, flow, flow_detail=None):
        return taskflow.engines.load(flow,
                                     flow_detail=flow_detail,
                                     engine='serial',
                                     backend=self.backend)


class MultiThreadedEngineTest(RetryTest,
                              RetryParallelExecutionTest,
                              test.TestCase):
    def _make_engine(self, flow, flow_detail=None, executor=None):
        return taskflow.engines.load(flow, flow_detail=flow_detail,
                                     engine='parallel',
                                     backend=self.backend,
                                     executor=executor)
