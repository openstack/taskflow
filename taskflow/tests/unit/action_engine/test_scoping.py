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

from taskflow.engines.action_engine import compiler
from taskflow.engines.action_engine import scopes as sc
from taskflow.patterns import graph_flow as gf
from taskflow.patterns import linear_flow as lf
from taskflow.patterns import unordered_flow as uf
from taskflow import test
from taskflow.tests import utils as test_utils


def _get_scopes(compilation, atom, names_only=True):
    walker = sc.ScopeWalker(compilation, atom, names_only=names_only)
    return list(iter(walker))


class LinearScopingTest(test.TestCase):
    def test_unknown(self):
        r = lf.Flow("root")
        r_1 = test_utils.TaskOneReturn("root.1")
        r.add(r_1)

        r_2 = test_utils.TaskOneReturn("root.2")
        c = compiler.PatternCompiler(r).compile()
        self.assertRaises(ValueError, _get_scopes, c, r_2)

    def test_empty(self):
        r = lf.Flow("root")
        r_1 = test_utils.TaskOneReturn("root.1")
        r.add(r_1)

        c = compiler.PatternCompiler(r).compile()
        self.assertIn(r_1, c.execution_graph)
        self.assertIsNotNone(c.hierarchy.find(r_1))

        walker = sc.ScopeWalker(c, r_1)
        scopes = list(walker)
        self.assertEqual([], scopes)

    def test_single_prior_linear(self):
        r = lf.Flow("root")
        r_1 = test_utils.TaskOneReturn("root.1")
        r_2 = test_utils.TaskOneReturn("root.2")
        r.add(r_1, r_2)

        c = compiler.PatternCompiler(r).compile()
        for a in r:
            self.assertIn(a, c.execution_graph)
            self.assertIsNotNone(c.hierarchy.find(a))

        self.assertEqual([], _get_scopes(c, r_1))
        self.assertEqual([['root.1']], _get_scopes(c, r_2))

    def test_nested_prior_linear(self):
        r = lf.Flow("root")
        r.add(test_utils.TaskOneReturn("root.1"),
              test_utils.TaskOneReturn("root.2"))
        sub_r = lf.Flow("subroot")
        sub_r_1 = test_utils.TaskOneReturn("subroot.1")
        sub_r.add(sub_r_1)
        r.add(sub_r)

        c = compiler.PatternCompiler(r).compile()
        self.assertEqual([[], ['root.2', 'root.1']], _get_scopes(c, sub_r_1))

    def test_nested_prior_linear_begin_middle_end(self):
        r = lf.Flow("root")
        begin_r = test_utils.TaskOneReturn("root.1")
        r.add(begin_r, test_utils.TaskOneReturn("root.2"))
        middle_r = test_utils.TaskOneReturn("root.3")
        r.add(middle_r)
        sub_r = lf.Flow("subroot")
        sub_r.add(test_utils.TaskOneReturn("subroot.1"),
                  test_utils.TaskOneReturn("subroot.2"))
        r.add(sub_r)
        end_r = test_utils.TaskOneReturn("root.4")
        r.add(end_r)

        c = compiler.PatternCompiler(r).compile()

        self.assertEqual([], _get_scopes(c, begin_r))
        self.assertEqual([['root.2', 'root.1']], _get_scopes(c, middle_r))
        self.assertEqual([['subroot.2', 'subroot.1', 'root.3', 'root.2',
                           'root.1']], _get_scopes(c, end_r))


class GraphScopingTest(test.TestCase):
    def test_dependent(self):
        r = gf.Flow("root")

        customer = test_utils.ProvidesRequiresTask("customer",
                                                   provides=['dog'],
                                                   requires=[])
        washer = test_utils.ProvidesRequiresTask("washer",
                                                 requires=['dog'],
                                                 provides=['wash'])
        dryer = test_utils.ProvidesRequiresTask("dryer",
                                                requires=['dog', 'wash'],
                                                provides=['dry_dog'])
        shaved = test_utils.ProvidesRequiresTask("shaver",
                                                 requires=['dry_dog'],
                                                 provides=['shaved_dog'])
        happy_customer = test_utils.ProvidesRequiresTask(
            "happy_customer", requires=['shaved_dog'], provides=['happiness'])

        r.add(customer, washer, dryer, shaved, happy_customer)

        c = compiler.PatternCompiler(r).compile()

        self.assertEqual([], _get_scopes(c, customer))
        self.assertEqual([['washer', 'customer']], _get_scopes(c, dryer))
        self.assertEqual([['shaver', 'dryer', 'washer', 'customer']],
                         _get_scopes(c, happy_customer))

    def test_no_visible(self):
        r = gf.Flow("root")
        atoms = []
        for i in range(0, 10):
            atoms.append(test_utils.TaskOneReturn("root.%s" % i))
        r.add(*atoms)

        c = compiler.PatternCompiler(r).compile()
        for a in atoms:
            self.assertEqual([], _get_scopes(c, a))

    def test_nested(self):
        r = gf.Flow("root")

        r_1 = test_utils.TaskOneReturn("root.1")
        r_2 = test_utils.TaskOneReturn("root.2")
        r.add(r_1, r_2)
        r.link(r_1, r_2)

        subroot = gf.Flow("subroot")
        subroot_r_1 = test_utils.TaskOneReturn("subroot.1")
        subroot_r_2 = test_utils.TaskOneReturn("subroot.2")
        subroot.add(subroot_r_1, subroot_r_2)
        subroot.link(subroot_r_1, subroot_r_2)

        r.add(subroot)
        r_3 = test_utils.TaskOneReturn("root.3")
        r.add(r_3)
        r.link(r_2, r_3)

        c = compiler.PatternCompiler(r).compile()
        self.assertEqual([], _get_scopes(c, r_1))
        self.assertEqual([['root.1']], _get_scopes(c, r_2))
        self.assertEqual([['root.2', 'root.1']], _get_scopes(c, r_3))

        self.assertEqual([], _get_scopes(c, subroot_r_1))
        self.assertEqual([['subroot.1']], _get_scopes(c, subroot_r_2))


class UnorderedScopingTest(test.TestCase):
    def test_no_visible(self):
        r = uf.Flow("root")
        atoms = []
        for i in range(0, 10):
            atoms.append(test_utils.TaskOneReturn("root.%s" % i))
        r.add(*atoms)
        c = compiler.PatternCompiler(r).compile()
        for a in atoms:
            self.assertEqual([], _get_scopes(c, a))


class MixedPatternScopingTest(test.TestCase):
    def test_graph_linear_scope(self):
        r = gf.Flow("root")
        r_1 = test_utils.TaskOneReturn("root.1")
        r_2 = test_utils.TaskOneReturn("root.2")
        r.add(r_1, r_2)
        r.link(r_1, r_2)

        s = lf.Flow("subroot")
        s_1 = test_utils.TaskOneReturn("subroot.1")
        s_2 = test_utils.TaskOneReturn("subroot.2")
        s.add(s_1, s_2)
        r.add(s)

        t = gf.Flow("subroot2")
        t_1 = test_utils.TaskOneReturn("subroot2.1")
        t_2 = test_utils.TaskOneReturn("subroot2.2")
        t.add(t_1, t_2)
        t.link(t_1, t_2)
        r.add(t)
        r.link(s, t)

        c = compiler.PatternCompiler(r).compile()
        self.assertEqual([], _get_scopes(c, r_1))
        self.assertEqual([['root.1']], _get_scopes(c, r_2))
        self.assertEqual([], _get_scopes(c, s_1))
        self.assertEqual([['subroot.1']], _get_scopes(c, s_2))
        self.assertEqual([[], ['subroot.2', 'subroot.1']],
                         _get_scopes(c, t_1))
        self.assertEqual([["subroot2.1"], ['subroot.2', 'subroot.1']],
                         _get_scopes(c, t_2))

    def test_linear_unordered_scope(self):
        r = lf.Flow("root")
        r_1 = test_utils.TaskOneReturn("root.1")
        r_2 = test_utils.TaskOneReturn("root.2")
        r.add(r_1, r_2)

        u = uf.Flow("subroot")
        atoms = []
        for i in range(0, 5):
            atoms.append(test_utils.TaskOneReturn("subroot.%s" % i))
        u.add(*atoms)
        r.add(u)

        r_3 = test_utils.TaskOneReturn("root.3")
        r.add(r_3)

        c = compiler.PatternCompiler(r).compile()

        self.assertEqual([], _get_scopes(c, r_1))
        self.assertEqual([['root.1']], _get_scopes(c, r_2))
        for a in atoms:
            self.assertEqual([[], ['root.2', 'root.1']], _get_scopes(c, a))

        scope = _get_scopes(c, r_3)
        self.assertEqual(1, len(scope))
        first_root = 0
        for i, n in enumerate(scope[0]):
            if n.startswith('root.'):
                first_root = i
                break
        first_subroot = 0
        for i, n in enumerate(scope[0]):
            if n.startswith('subroot.'):
                first_subroot = i
                break
        self.assertGreater(first_subroot, first_root)
        self.assertEqual(['root.2', 'root.1'], scope[0][-2:])

    def test_shadow_graph(self):
        r = gf.Flow("root")
        customer = test_utils.ProvidesRequiresTask("customer",
                                                   provides=['dog'],
                                                   requires=[])
        customer2 = test_utils.ProvidesRequiresTask("customer2",
                                                    provides=['dog'],
                                                    requires=[])
        washer = test_utils.ProvidesRequiresTask("washer",
                                                 requires=['dog'],
                                                 provides=['wash'])
        r.add(customer, washer)
        r.add(customer2, resolve_requires=False)
        r.link(customer2, washer)

        c = compiler.PatternCompiler(r).compile()

        # The order currently is *not* guaranteed to be 'customer' before
        # 'customer2' or the reverse, since either can occur before the
        # washer; since *either* is a valid topological ordering of the
        # dependencies...
        #
        # This may be different after/if the following is resolved:
        #
        # https://github.com/networkx/networkx/issues/1181 (and a few others)
        self.assertEqual(set(['customer', 'customer2']),
                         set(_get_scopes(c, washer)[0]))
        self.assertEqual([], _get_scopes(c, customer2))
        self.assertEqual([], _get_scopes(c, customer))

    def test_shadow_linear(self):
        r = lf.Flow("root")

        customer = test_utils.ProvidesRequiresTask("customer",
                                                   provides=['dog'],
                                                   requires=[])
        customer2 = test_utils.ProvidesRequiresTask("customer2",
                                                    provides=['dog'],
                                                    requires=[])
        washer = test_utils.ProvidesRequiresTask("washer",
                                                 requires=['dog'],
                                                 provides=['wash'])
        r.add(customer, customer2, washer)

        c = compiler.PatternCompiler(r).compile()

        # This order is guaranteed...
        self.assertEqual(['customer2', 'customer'], _get_scopes(c, washer)[0])
