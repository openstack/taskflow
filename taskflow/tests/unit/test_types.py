# -*- coding: utf-8 -*-

#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
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

import time

import networkx as nx
import six

from taskflow import exceptions as excp
from taskflow import test
from taskflow.types import fsm
from taskflow.types import graph
from taskflow.types import latch
from taskflow.types import periodic
from taskflow.types import table
from taskflow.types import timing as tt
from taskflow.types import tree
from taskflow.utils import threading_utils as tu


class PeriodicThingy(object):
    def __init__(self):
        self.capture = []

    @periodic.periodic(0.01)
    def a(self):
        self.capture.append('a')

    @periodic.periodic(0.02)
    def b(self):
        self.capture.append('b')

    def c(self):
        pass

    def d(self):
        pass


class GraphTest(test.TestCase):
    def test_no_successors_no_predecessors(self):
        g = graph.DiGraph()
        g.add_node("a")
        g.add_node("b")
        g.add_node("c")
        g.add_edge("b", "c")
        self.assertEqual(set(['a', 'b']),
                         set(g.no_predecessors_iter()))
        self.assertEqual(set(['a', 'c']),
                         set(g.no_successors_iter()))

    def test_directed(self):
        g = graph.DiGraph()
        g.add_node("a")
        g.add_node("b")
        g.add_edge("a", "b")
        self.assertTrue(g.is_directed_acyclic())
        g.add_edge("b", "a")
        self.assertFalse(g.is_directed_acyclic())

    def test_frozen(self):
        g = graph.DiGraph()
        self.assertFalse(g.frozen)
        g.add_node("b")
        g.freeze()
        self.assertRaises(nx.NetworkXError, g.add_node, "c")


class TreeTest(test.TestCase):
    def _make_species(self):
        # This is the following tree:
        #
        # animal
        # |__mammal
        # |  |__horse
        # |  |__primate
        # |     |__monkey
        # |     |__human
        # |__reptile
        a = tree.Node("animal")
        m = tree.Node("mammal")
        r = tree.Node("reptile")
        a.add(m)
        a.add(r)
        m.add(tree.Node("horse"))
        p = tree.Node("primate")
        m.add(p)
        p.add(tree.Node("monkey"))
        p.add(tree.Node("human"))
        return a

    def test_path(self):
        root = self._make_species()
        human = root.find("human")
        self.assertIsNotNone(human)
        p = list([n.item for n in human.path_iter()])
        self.assertEqual(['human', 'primate', 'mammal', 'animal'], p)

    def test_empty(self):
        root = tree.Node("josh")
        self.assertTrue(root.empty())

    def test_not_empty(self):
        root = self._make_species()
        self.assertFalse(root.empty())

    def test_node_count(self):
        root = self._make_species()
        self.assertEqual(7, 1 + root.child_count(only_direct=False))

    def test_index(self):
        root = self._make_species()
        self.assertEqual(0, root.index("mammal"))
        self.assertEqual(1, root.index("reptile"))

    def test_contains(self):
        root = self._make_species()
        self.assertIn("monkey", root)
        self.assertNotIn("bird", root)

    def test_freeze(self):
        root = self._make_species()
        root.freeze()
        self.assertRaises(tree.FrozenNode, root.add, "bird")

    def test_dfs_itr(self):
        root = self._make_species()
        things = list([n.item for n in root.dfs_iter(include_self=True)])
        self.assertEqual(set(['animal', 'reptile', 'mammal', 'horse',
                              'primate', 'monkey', 'human']), set(things))

    def test_dfs_itr_order(self):
        root = self._make_species()
        things = list([n.item for n in root.dfs_iter(include_self=True)])
        self.assertEqual(['animal', 'mammal', 'horse', 'primate',
                          'monkey', 'human', 'reptile'], things)
        things = list([n.item for n in root.dfs_iter(include_self=False)])
        self.assertEqual(['mammal', 'horse', 'primate',
                          'monkey', 'human', 'reptile'], things)

    def test_bfs_iter(self):
        root = self._make_species()
        things = list([n.item for n in root.bfs_iter(include_self=True)])
        self.assertEqual(['animal', 'reptile', 'mammal', 'primate',
                          'horse', 'human', 'monkey'], things)
        things = list([n.item for n in root.bfs_iter(include_self=False)])
        self.assertEqual(['reptile', 'mammal', 'primate',
                          'horse', 'human', 'monkey'], things)


class StopWatchTest(test.TestCase):
    def setUp(self):
        super(StopWatchTest, self).setUp()
        tt.StopWatch.set_now_override(now=0)
        self.addCleanup(tt.StopWatch.clear_overrides)

    def test_leftover_no_duration(self):
        watch = tt.StopWatch()
        watch.start()
        self.assertRaises(RuntimeError, watch.leftover)
        self.assertRaises(RuntimeError, watch.leftover, return_none=False)
        self.assertIsNone(watch.leftover(return_none=True))

    def test_no_states(self):
        watch = tt.StopWatch()
        self.assertRaises(RuntimeError, watch.stop)
        self.assertRaises(RuntimeError, watch.resume)

    def test_bad_expiry(self):
        self.assertRaises(ValueError, tt.StopWatch, -1)

    def test_backwards(self):
        watch = tt.StopWatch(0.1)
        watch.start()
        tt.StopWatch.advance_time_seconds(0.5)
        self.assertTrue(watch.expired())

        tt.StopWatch.advance_time_seconds(-1.0)
        self.assertFalse(watch.expired())
        self.assertEqual(0.0, watch.elapsed())

    def test_expiry(self):
        watch = tt.StopWatch(0.1)
        watch.start()
        tt.StopWatch.advance_time_seconds(0.2)
        self.assertTrue(watch.expired())

    def test_not_expired(self):
        watch = tt.StopWatch(0.1)
        watch.start()
        tt.StopWatch.advance_time_seconds(0.05)
        self.assertFalse(watch.expired())

    def test_no_expiry(self):
        watch = tt.StopWatch(0.1)
        self.assertRaises(RuntimeError, watch.expired)

    def test_elapsed(self):
        watch = tt.StopWatch()
        watch.start()
        tt.StopWatch.advance_time_seconds(0.2)
        # NOTE(harlowja): Allow for a slight variation by using 0.19.
        self.assertGreaterEqual(0.19, watch.elapsed())

    def test_no_elapsed(self):
        watch = tt.StopWatch()
        self.assertRaises(RuntimeError, watch.elapsed)

    def test_no_leftover(self):
        watch = tt.StopWatch()
        self.assertRaises(RuntimeError, watch.leftover)
        watch = tt.StopWatch(1)
        self.assertRaises(RuntimeError, watch.leftover)

    def test_pause_resume(self):
        watch = tt.StopWatch()
        watch.start()
        tt.StopWatch.advance_time_seconds(0.05)
        watch.stop()
        elapsed = watch.elapsed()
        self.assertAlmostEqual(elapsed, watch.elapsed())
        watch.resume()
        tt.StopWatch.advance_time_seconds(0.05)
        self.assertNotEqual(elapsed, watch.elapsed())

    def test_context_manager(self):
        with tt.StopWatch() as watch:
            tt.StopWatch.advance_time_seconds(0.05)
        self.assertGreater(0.01, watch.elapsed())

    def test_splits(self):
        watch = tt.StopWatch()
        watch.start()
        self.assertEqual(0, len(watch.splits))

        watch.split()
        self.assertEqual(1, len(watch.splits))
        self.assertEqual(watch.splits[0].elapsed,
                         watch.splits[0].length)

        tt.StopWatch.advance_time_seconds(0.05)
        watch.split()
        splits = watch.splits
        self.assertEqual(2, len(splits))
        self.assertNotEqual(splits[0].elapsed, splits[1].elapsed)
        self.assertEqual(splits[1].length,
                         splits[1].elapsed - splits[0].elapsed)

        watch.stop()
        self.assertEqual(2, len(watch.splits))

        watch.start()
        self.assertEqual(0, len(watch.splits))

    def test_elapsed_maximum(self):
        watch = tt.StopWatch()
        watch.start()

        tt.StopWatch.advance_time_seconds(1)
        self.assertEqual(1, watch.elapsed())

        tt.StopWatch.advance_time_seconds(10)
        self.assertEqual(11, watch.elapsed())
        self.assertEqual(1, watch.elapsed(maximum=1))

        watch.stop()
        self.assertEqual(11, watch.elapsed())
        tt.StopWatch.advance_time_seconds(10)
        self.assertEqual(11, watch.elapsed())
        self.assertEqual(0, watch.elapsed(maximum=-1))


class TableTest(test.TestCase):
    def test_create_valid_no_rows(self):
        tbl = table.PleasantTable(['Name', 'City', 'State', 'Country'])
        self.assertGreater(0, len(tbl.pformat()))

    def test_create_valid_rows(self):
        tbl = table.PleasantTable(['Name', 'City', 'State', 'Country'])
        before_rows = tbl.pformat()
        tbl.add_row(["Josh", "San Jose", "CA", "USA"])
        after_rows = tbl.pformat()
        self.assertGreater(len(before_rows), len(after_rows))

    def test_create_invalid_columns(self):
        self.assertRaises(ValueError, table.PleasantTable, [])

    def test_create_invalid_rows(self):
        tbl = table.PleasantTable(['Name', 'City', 'State', 'Country'])
        self.assertRaises(ValueError, tbl.add_row, ['a', 'b'])


class FSMTest(test.TestCase):
    def setUp(self):
        super(FSMTest, self).setUp()
        # NOTE(harlowja): this state machine will never stop if run() is used.
        self.jumper = fsm.FSM("down")
        self.jumper.add_state('up')
        self.jumper.add_state('down')
        self.jumper.add_transition('down', 'up', 'jump')
        self.jumper.add_transition('up', 'down', 'fall')
        self.jumper.add_reaction('up', 'jump', lambda *args: 'fall')
        self.jumper.add_reaction('down', 'fall', lambda *args: 'jump')

    def test_bad_start_state(self):
        m = fsm.FSM('unknown')
        self.assertRaises(excp.NotFound, m.run, 'unknown')

    def test_contains(self):
        m = fsm.FSM('unknown')
        self.assertNotIn('unknown', m)
        m.add_state('unknown')
        self.assertIn('unknown', m)

    def test_duplicate_state(self):
        m = fsm.FSM('unknown')
        m.add_state('unknown')
        self.assertRaises(excp.Duplicate, m.add_state, 'unknown')

    def test_duplicate_reaction(self):
        self.assertRaises(
            # Currently duplicate reactions are not allowed...
            excp.Duplicate,
            self.jumper.add_reaction, 'down', 'fall', lambda *args: 'skate')

    def test_bad_transition(self):
        m = fsm.FSM('unknown')
        m.add_state('unknown')
        m.add_state('fire')
        self.assertRaises(excp.NotFound, m.add_transition,
                          'unknown', 'something', 'boom')
        self.assertRaises(excp.NotFound, m.add_transition,
                          'something', 'unknown', 'boom')

    def test_bad_reaction(self):
        m = fsm.FSM('unknown')
        m.add_state('unknown')
        self.assertRaises(excp.NotFound, m.add_reaction, 'something', 'boom',
                          lambda *args: 'cough')

    def test_run(self):
        m = fsm.FSM('down')
        m.add_state('down')
        m.add_state('up')
        m.add_state('broken', terminal=True)
        m.add_transition('down', 'up', 'jump')
        m.add_transition('up', 'broken', 'hit-wall')
        m.add_reaction('up', 'jump', lambda *args: 'hit-wall')
        self.assertEqual(['broken', 'down', 'up'], sorted(m.states))
        self.assertEqual(2, m.events)
        m.initialize()
        self.assertEqual('down', m.current_state)
        self.assertFalse(m.terminated)
        m.run('jump')
        self.assertTrue(m.terminated)
        self.assertEqual('broken', m.current_state)
        self.assertRaises(excp.InvalidState, m.run, 'jump', initialize=False)

    def test_on_enter_on_exit(self):
        enter_transitions = []
        exit_transitions = []

        def on_exit(state, event):
            exit_transitions.append((state, event))

        def on_enter(state, event):
            enter_transitions.append((state, event))

        m = fsm.FSM('start')
        m.add_state('start', on_exit=on_exit)
        m.add_state('down', on_enter=on_enter, on_exit=on_exit)
        m.add_state('up', on_enter=on_enter, on_exit=on_exit)
        m.add_transition('start', 'down', 'beat')
        m.add_transition('down', 'up', 'jump')
        m.add_transition('up', 'down', 'fall')

        m.initialize()
        m.process_event('beat')
        m.process_event('jump')
        m.process_event('fall')
        self.assertEqual([('down', 'beat'),
                          ('up', 'jump'), ('down', 'fall')], enter_transitions)
        self.assertEqual(
            [('start', 'beat'), ('down', 'jump'), ('up', 'fall')],
            exit_transitions)

    def test_run_iter(self):
        up_downs = []
        for (old_state, new_state) in self.jumper.run_iter('jump'):
            up_downs.append((old_state, new_state))
            if len(up_downs) >= 3:
                break
        self.assertEqual([('down', 'up'), ('up', 'down'), ('down', 'up')],
                         up_downs)
        self.assertFalse(self.jumper.terminated)
        self.assertEqual('up', self.jumper.current_state)
        self.jumper.process_event('fall')
        self.assertEqual('down', self.jumper.current_state)

    def test_run_send(self):
        up_downs = []
        it = self.jumper.run_iter('jump')
        while True:
            up_downs.append(it.send(None))
            if len(up_downs) >= 3:
                it.close()
                break
        self.assertEqual('up', self.jumper.current_state)
        self.assertFalse(self.jumper.terminated)
        self.assertEqual([('down', 'up'), ('up', 'down'), ('down', 'up')],
                         up_downs)
        self.assertRaises(StopIteration, six.next, it)

    def test_run_send_fail(self):
        up_downs = []
        it = self.jumper.run_iter('jump')
        up_downs.append(six.next(it))
        self.assertRaises(excp.NotFound, it.send, 'fail')
        it.close()
        self.assertEqual([('down', 'up')], up_downs)

    def test_not_initialized(self):
        self.assertRaises(fsm.NotInitialized,
                          self.jumper.process_event, 'jump')

    def test_copy_states(self):
        c = fsm.FSM('down')
        self.assertEqual(0, len(c.states))
        d = c.copy()
        c.add_state('up')
        c.add_state('down')
        self.assertEqual(2, len(c.states))
        self.assertEqual(0, len(d.states))

    def test_copy_reactions(self):
        c = fsm.FSM('down')
        d = c.copy()

        c.add_state('down')
        c.add_state('up')
        c.add_reaction('down', 'jump', lambda *args: 'up')
        c.add_transition('down', 'up', 'jump')

        self.assertEqual(1, c.events)
        self.assertEqual(0, d.events)
        self.assertNotIn('down', d)
        self.assertNotIn('up', d)
        self.assertEqual([], list(d))
        self.assertEqual([('down', 'jump', 'up')], list(c))

    def test_copy_initialized(self):
        j = self.jumper.copy()
        self.assertIsNone(j.current_state)

        for i, transition in enumerate(self.jumper.run_iter('jump')):
            if i == 4:
                break

        self.assertIsNone(j.current_state)
        self.assertIsNotNone(self.jumper.current_state)

    def test_iter(self):
        transitions = list(self.jumper)
        self.assertEqual(2, len(transitions))
        self.assertIn(('up', 'fall', 'down'), transitions)
        self.assertIn(('down', 'jump', 'up'), transitions)

    def test_freeze(self):
        self.jumper.freeze()
        self.assertRaises(fsm.FrozenMachine, self.jumper.add_state, 'test')
        self.assertRaises(fsm.FrozenMachine,
                          self.jumper.add_transition, 'test', 'test', 'test')
        self.assertRaises(fsm.FrozenMachine,
                          self.jumper.add_reaction,
                          'test', 'test', lambda *args: 'test')

    def test_invalid_callbacks(self):
        m = fsm.FSM('working')
        m.add_state('working')
        m.add_state('broken')
        self.assertRaises(ValueError, m.add_state, 'b', on_enter=2)
        self.assertRaises(ValueError, m.add_state, 'b', on_exit=2)


class PeriodicTest(test.TestCase):

    def test_invalid_periodic(self):

        def no_op():
            pass

        self.assertRaises(ValueError, periodic.periodic, -1)

    def test_valid_periodic(self):

        @periodic.periodic(2)
        def no_op():
            pass

        self.assertTrue(getattr(no_op, '_periodic'))
        self.assertEqual(2, getattr(no_op, '_periodic_spacing'))
        self.assertEqual(True, getattr(no_op, '_periodic_run_immediately'))

    def test_scanning_periodic(self):
        p = PeriodicThingy()
        w = periodic.PeriodicWorker.create([p])
        self.assertEqual(2, len(w))

        t = tu.daemon_thread(target=w.start)
        t.start()
        time.sleep(0.1)
        w.stop()
        t.join()

        b_calls = [c for c in p.capture if c == 'b']
        self.assertGreater(0, len(b_calls))
        a_calls = [c for c in p.capture if c == 'a']
        self.assertGreater(0, len(a_calls))

    def test_periodic_single(self):
        barrier = latch.Latch(5)
        capture = []
        tombstone = tu.Event()

        @periodic.periodic(0.01)
        def callee():
            barrier.countdown()
            if barrier.needed == 0:
                tombstone.set()
            capture.append(1)

        w = periodic.PeriodicWorker([callee], tombstone=tombstone)
        t = tu.daemon_thread(target=w.start)
        t.start()
        t.join()

        self.assertEqual(0, barrier.needed)
        self.assertEqual(5, sum(capture))
        self.assertTrue(tombstone.is_set())

    def test_immediate(self):
        capture = []

        @periodic.periodic(120, run_immediately=True)
        def a():
            capture.append('a')

        w = periodic.PeriodicWorker([a])
        t = tu.daemon_thread(target=w.start)
        t.start()
        time.sleep(0.1)
        w.stop()
        t.join()

        a_calls = [c for c in capture if c == 'a']
        self.assertGreater(0, len(a_calls))

    def test_period_double_no_immediate(self):
        capture = []

        @periodic.periodic(0.01, run_immediately=False)
        def a():
            capture.append('a')

        @periodic.periodic(0.02, run_immediately=False)
        def b():
            capture.append('b')

        w = periodic.PeriodicWorker([a, b])
        t = tu.daemon_thread(target=w.start)
        t.start()
        time.sleep(0.1)
        w.stop()
        t.join()

        b_calls = [c for c in capture if c == 'b']
        self.assertGreater(0, len(b_calls))
        a_calls = [c for c in capture if c == 'a']
        self.assertGreater(0, len(a_calls))

    def test_start_nothing_error(self):
        w = periodic.PeriodicWorker([])
        self.assertRaises(RuntimeError, w.start)

    def test_missing_function_attrs(self):

        def fake_periodic():
            pass

        cb = fake_periodic
        self.assertRaises(ValueError, periodic.PeriodicWorker, [cb])
