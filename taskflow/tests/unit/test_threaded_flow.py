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

import threading
import time

from taskflow import decorators
from taskflow import exceptions as excp
from taskflow import states

from taskflow.patterns import threaded_flow as tf
from taskflow import test
from taskflow.tests import utils


def _find_idx(what, search_where):
    for i, j in enumerate(search_where):
        if i == what:
            return j
    return -1


class ThreadedFlowTest(test.TestCase):
    def _make_tracking_flow(self, name):
        notify_lock = threading.RLock()
        flo = tf.Flow(name)
        notifications = []

        def save_notify(state, details):
            runner = details.get('runner')
            if not runner:
                return
            with notify_lock:
                notifications.append((runner.uuid, state, dict(details)))

        flo.task_notifier.register('*', save_notify)
        return (flo, notifications)

    def _make_watched_flow(self, name):
        history_lock = threading.RLock()
        flo = tf.Flow(name)
        history = {}

        def save_state(state, details):
            runner = details.get('runner')
            if not runner:
                return
            with history_lock:
                old_state = details.get('old_state')
                old_states = history.get(runner.uuid, [])
                if not old_states:
                    old_states.append(old_state)
                old_states.append(state)
                history[runner.uuid] = old_states

        flo.task_notifier.register('*', save_state)
        return (flo, history)

    def test_somewhat_complicated(self):
        """Tests a somewhat complicated dependency graph.

            X--Y--C--D
                  |  |
            A--B--    --G--
                  |  |     |--Z(end)
            E--F--    --H--
        """
        (flo, notifications) = self._make_tracking_flow("sanity-test")

        # X--Y
        x = flo.add(utils.ProvidesRequiresTask("X",
                                               provides=['x'],
                                               requires=[]))
        y = flo.add(utils.ProvidesRequiresTask("Y",
                                               provides=['y'],
                                               requires=['x']))

        # A--B
        a = flo.add(utils.ProvidesRequiresTask("A",
                                               provides=['a'],
                                               requires=[]))
        b = flo.add(utils.ProvidesRequiresTask("B",
                                               provides=['b'],
                                               requires=['a']))

        # E--F
        e = flo.add(utils.ProvidesRequiresTask("E",
                                               provides=['e'],
                                               requires=[]))
        f = flo.add(utils.ProvidesRequiresTask("F",
                                               provides=['f'],
                                               requires=['e']))

        # C--D
        c = flo.add(utils.ProvidesRequiresTask("C",
                                               provides=['c'],
                                               requires=['f', 'b', 'y']))
        d = flo.add(utils.ProvidesRequiresTask("D",
                                               provides=['d'],
                                               requires=['c']))

        # G
        g = flo.add(utils.ProvidesRequiresTask("G",
                                               provides=['g'],
                                               requires=['d']))

        # H
        h = flo.add(utils.ProvidesRequiresTask("H",
                                               provides=['h'],
                                               requires=['d']))

        # Z
        z = flo.add(utils.ProvidesRequiresTask("Z",
                                               provides=['z'],
                                               requires=['g', 'h']))

        all_uuids = [z, h, g, d, c, f, e, b, a, y, x]
        self.assertEquals(states.PENDING, flo.state)
        flo.run({})
        self.assertEquals(states.SUCCESS, flo.state)

        # Analyze the notifications to determine that the correct ordering
        # occurred

        # Discard states we aren't really interested in.
        c_notifications = []
        uuids_ran = set()
        for (uuid, state, details) in notifications:
            if state not in [states.RUNNING, states.SUCCESS, states.FAILURE]:
                continue
            uuids_ran.add(uuid)
            c_notifications.append((uuid, state, details))
        notifications = c_notifications
        self.assertEquals(len(all_uuids), len(uuids_ran))

        # Select out the run order
        just_ran_uuids = []
        for (uuid, state, details) in notifications:
            if state not in [states.RUNNING]:
                continue
            just_ran_uuids.append(uuid)

        def ran_before(ran_uuid, before_what):
            before_idx = just_ran_uuids.index(ran_uuid)
            other_idxs = [just_ran_uuids.index(u) for u in before_what]
            was_before = True
            for idx in other_idxs:
                if idx < before_idx:
                    was_before = False
            return was_before

        def ran_after(ran_uuid, after_what):
            after_idx = just_ran_uuids.index(ran_uuid)
            other_idxs = [just_ran_uuids.index(u) for u in after_what]
            was_after = True
            for idx in other_idxs:
                if idx > after_idx:
                    was_after = False
            return was_after

        # X, A, E should always run before the others
        self.assertTrue(ran_before(x, [c, d, g, h, z]))
        self.assertTrue(ran_before(a, [c, d, g, h, z]))
        self.assertTrue(ran_before(e, [c, d, g, h, z]))

        # Y, B, F should always run before C
        self.assertTrue(ran_before(y, [c]))
        self.assertTrue(ran_before(b, [c]))
        self.assertTrue(ran_before(f, [c]))

        # C runs before D
        self.assertTrue(ran_before(c, [d]))

        # G and H are before Z
        self.assertTrue(ran_before(g, [z]))
        self.assertTrue(ran_before(h, [z]))

        # C, D runs after X, Y, B, E, F
        self.assertTrue(ran_after(c, [x, y, b, e, f]))
        self.assertTrue(ran_after(d, [x, y, b, c, e, f]))

        # Z is last
        all_uuids_no_z = list(all_uuids)
        all_uuids_no_z.remove(z)
        self.assertTrue(ran_after(z, all_uuids_no_z))

    def test_empty_cancel(self):
        (flo, history) = self._make_watched_flow("sanity-test")
        self.assertEquals(states.PENDING, flo.state)
        flo.cancel()
        self.assertEquals(states.CANCELLED, flo.state)

    def test_self_loop_flo(self):
        (flo, history) = self._make_watched_flow("sanity-test")
        flo.add(utils.ProvidesRequiresTask("do-that",
                                           provides=['c'],
                                           requires=['c']))
        self.assertRaises(excp.InvalidStateException, flo.run, {})

    def test_circular_flo(self):
        (flo, history) = self._make_watched_flow("sanity-test")
        flo.add(utils.ProvidesRequiresTask("do-that",
                                           provides=['c'],
                                           requires=['a']))
        flo.add(utils.ProvidesRequiresTask("do-this",
                                           provides=['a'],
                                           requires=['c']))
        self.assertRaises(excp.InvalidStateException, flo.run, {})

    def test_no_input_flo(self):
        (flo, history) = self._make_watched_flow("sanity-test")
        flo.add(utils.ProvidesRequiresTask("do-that",
                                           provides=['c'],
                                           requires=['a']))
        flo.add(utils.ProvidesRequiresTask("do-this",
                                           provides=['b'],
                                           requires=['c']))
        self.assertRaises(excp.InvalidStateException, flo.run, {})

    def test_simple_resume(self):
        (flo, history) = self._make_watched_flow("sanity-test")
        f_uuid = flo.add(utils.ProvidesRequiresTask("do-this",
                                                    provides=['a'],
                                                    requires=[]))
        flo.add(utils.ProvidesRequiresTask("do-that",
                                           provides=['c'],
                                           requires=['a']))

        def resume_it(flow, ordering):
            ran_already = []
            not_ran = []
            for r in ordering:
                if r.uuid == f_uuid:
                    ran_already.append((r, {
                        'result': 'b',
                        'states': [states.SUCCESS],
                    }))
                else:
                    not_ran.append(r)
            return (ran_already, not_ran)

        flo.resumer = resume_it
        flo.run({})
        self.assertEquals('b', flo.results[f_uuid])
        self.assertEquals(states.SUCCESS, flo.state)

    def test_active_cancel(self):
        (flo, history) = self._make_watched_flow("sanity-test")
        flo.add(utils.ProvidesRequiresTask("do-this",
                                           provides=['a'],
                                           requires=[]))
        flo.add(utils.ProvidesRequiresTask("do-that",
                                           provides=['c'],
                                           requires=['a']))

        @decorators.task(provides=['d'], requires=['c'])
        def cancel_it(context, c):
            am_cancelled = flo.cancel()
            return am_cancelled

        uuid = flo.add(cancel_it)
        flo.add(utils.ProvidesRequiresTask("do-the-other",
                                           provides=['e'],
                                           requires=['d']))

        flo.run({})
        self.assertIn(uuid, flo.results)
        self.assertEquals(states.INCOMPLETE, flo.state)
        self.assertEquals(1, flo.results[uuid])

    def test_sanity_run(self):
        (flo, history) = self._make_watched_flow("sanity-test")
        flo.add(utils.ProvidesRequiresTask("do-this",
                                           provides=['a'],
                                           requires=[]))
        flo.add(utils.ProvidesRequiresTask("do-that",
                                           provides=['c'],
                                           requires=['a']))
        flo.add(utils.ProvidesRequiresTask("do-other",
                                           provides=['d'],
                                           requires=[]))
        flo.add(utils.ProvidesRequiresTask("do-thing",
                                           provides=['e'],
                                           requires=['d']))
        self.assertEquals(states.PENDING, flo.state)
        context = {}
        flo.run(context)
        self.assertEquals(states.SUCCESS, flo.state)
        self.assertTrue(len(context) > 0)
        # Even when running in parallel this will be the required order since
        # 'do-that' depends on 'do-this' finishing first.
        expected_order = ['do-this', 'do-that']
        this_that = [t for t in context[utils.ORDER_KEY]
                     if t in expected_order]
        self.assertEquals(expected_order, this_that)
        expected_order = ['do-other', 'do-thing']
        this_that = [t for t in context[utils.ORDER_KEY]
                     if t in expected_order]
        self.assertEquals(expected_order, this_that)

    def test_single_failure(self):

        def reverter(context, result, cause):
            context['reverted'] = True

        @decorators.task(revert=reverter)
        def fail_quick(context):
            raise IOError("Broken")

        (flo, history) = self._make_watched_flow('test-single-fail')
        f_uuid = flo.add(fail_quick)
        context = {}
        self.assertRaises(IOError, flo.run, context)
        self.assertEquals(states.FAILURE, flo.state)
        self.assertEquals(states.REVERTED, history[f_uuid][-1])
        self.assertTrue(context.get('reverted'))

    def test_failure_cancel_successors(self):
        (flo, history) = self._make_watched_flow("failure-cancel")

        @decorators.task(provides=['b', 'c'])
        def fail_quick(context):
            raise IOError("Broken")

        @decorators.task
        def after_fail(context, b):
            pass

        @decorators.task
        def after_fail2(context, c):
            pass

        fq, af, af2 = flo.add_many([fail_quick, after_fail, after_fail2])
        self.assertEquals(states.PENDING, flo.state)

        context = {}
        self.assertRaises(IOError, flo.run, context)
        self.assertEquals(states.FAILURE, flo.state)
        self.assertEquals(states.REVERTED, history[fq][-1])
        self.assertEquals(states.CANCELLED, history[af][-1])
        self.assertEquals(states.CANCELLED, history[af2][-1])

    def test_live_timeout(self):

        @decorators.task(provides=['a'])
        def task_long(context):
            time.sleep(1)
            return {
                'a': 2,
            }

        @decorators.task(provides=['b'])
        def wait_short(context, a):
            pass

        @decorators.task
        def wait_ok_long(context, a):
            pass

        @decorators.task
        def wait_after_short(context, b):
            pass

        (flo, history) = self._make_watched_flow('test-live')
        flo.add(task_long)
        ws_uuid = flo.add(wait_short, timeout=0.1)
        flo.add(wait_ok_long)
        was_uuid = flo.add(wait_after_short)

        flo.run({})
        self.assertEquals(states.INCOMPLETE, flo.state)
        self.assertEquals(states.TIMED_OUT, history[ws_uuid][-1])
        self.assertEquals(states.CANCELLED, history[was_uuid][-1])
