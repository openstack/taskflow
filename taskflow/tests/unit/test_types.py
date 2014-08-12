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

from taskflow import test
from taskflow.types import graph
from taskflow.types import timing as tt
from taskflow.types import tree


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


class StopWatchUtilsTest(test.TestCase):
    def test_no_states(self):
        watch = tt.StopWatch()
        self.assertRaises(RuntimeError, watch.stop)
        self.assertRaises(RuntimeError, watch.resume)

    def test_expiry(self):
        watch = tt.StopWatch(0.1)
        watch.start()
        time.sleep(0.2)
        self.assertTrue(watch.expired())

    def test_no_expiry(self):
        watch = tt.StopWatch(0.1)
        watch.start()
        self.assertFalse(watch.expired())

    def test_elapsed(self):
        watch = tt.StopWatch()
        watch.start()
        time.sleep(0.2)
        # NOTE(harlowja): Allow for a slight variation by using 0.19.
        self.assertGreaterEqual(0.19, watch.elapsed())

    def test_pause_resume(self):
        watch = tt.StopWatch()
        watch.start()
        time.sleep(0.05)
        watch.stop()
        elapsed = watch.elapsed()
        time.sleep(0.05)
        self.assertAlmostEqual(elapsed, watch.elapsed())
        watch.resume()
        self.assertNotEqual(elapsed, watch.elapsed())

    def test_context_manager(self):
        with tt.StopWatch() as watch:
            time.sleep(0.05)
        self.assertGreater(0.01, watch.elapsed())
