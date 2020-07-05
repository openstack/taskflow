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

from taskflow import engines
from taskflow.engines.action_engine import compiler
from taskflow import exceptions as exc
from taskflow.patterns import graph_flow as gf
from taskflow.patterns import linear_flow as lf
from taskflow.patterns import unordered_flow as uf
from taskflow import retry
from taskflow import test
from taskflow.tests import utils as test_utils


def _replicate_graph_with_names(compilation):
    # Turn a graph of nodes into a graph of names only so that
    # testing can use those names instead of having to use the exact
    # node objects themselves (which is problematic for any end nodes that
    # are added into the graph *dynamically*, and are not there in the
    # original/source flow).
    g = compilation.execution_graph
    n_g = g.__class__(name=g.name)
    for node, node_data in g.nodes(data=True):
        n_g.add_node(node.name, attr_dict=node_data)
    for u, v, u_v_data in g.edges(data=True):
        n_g.add_edge(u.name, v.name, attr_dict=u_v_data)
    return n_g


class PatternCompileTest(test.TestCase):
    def test_task(self):
        task = test_utils.DummyTask(name='a')
        g = _replicate_graph_with_names(
            compiler.PatternCompiler(task).compile())
        self.assertEqual(['a'], list(g.nodes()))
        self.assertEqual([], list(g.edges()))

    def test_retry(self):
        r = retry.AlwaysRevert('r1')
        self.assertRaises(TypeError, compiler.PatternCompiler(r).compile)

    def test_wrong_object(self):
        msg_regex = '^Unknown object .* requested to compile'
        self.assertRaisesRegex(TypeError, msg_regex,
                               compiler.PatternCompiler(42).compile)

    def test_empty(self):
        flo = lf.Flow("test")
        compiler.PatternCompiler(flo).compile()

    def test_linear(self):
        a, b, c, d = test_utils.make_many(4)
        flo = lf.Flow("test")
        flo.add(a, b, c)
        inner_flo = lf.Flow("sub-test")
        inner_flo.add(d)
        flo.add(inner_flo)

        g = _replicate_graph_with_names(
            compiler.PatternCompiler(flo).compile())
        self.assertEqual(8, len(g))

        order = list(g.topological_sort())
        self.assertEqual(['test', 'a', 'b', 'c',
                          "sub-test", 'd', "sub-test[$]",
                          'test[$]'], order)
        self.assertTrue(g.has_edge('c', "sub-test"))
        self.assertTrue(g.has_edge("sub-test", 'd'))
        self.assertEqual({'invariant': True},
                         g.get_edge_data("sub-test", 'd'))
        self.assertEqual(['test[$]'], list(g.no_successors_iter()))
        self.assertEqual(['test'], list(g.no_predecessors_iter()))

    def test_invalid(self):
        a, b, c = test_utils.make_many(3)
        flo = lf.Flow("test")
        flo.add(a, b, c)
        flo.add(flo)
        self.assertRaises(ValueError,
                          compiler.PatternCompiler(flo).compile)

    def test_unordered(self):
        a, b, c, d = test_utils.make_many(4)
        flo = uf.Flow("test")
        flo.add(a, b, c, d)

        g = _replicate_graph_with_names(
            compiler.PatternCompiler(flo).compile())
        self.assertEqual(6, len(g))
        self.assertCountEqual(g.edges(), [
            ('test', 'a'),
            ('test', 'b'),
            ('test', 'c'),
            ('test', 'd'),
            ('a', 'test[$]'),
            ('b', 'test[$]'),
            ('c', 'test[$]'),
            ('d', 'test[$]'),
        ])
        self.assertEqual(set(['test']), set(g.no_predecessors_iter()))

    def test_linear_nested(self):
        a, b, c, d = test_utils.make_many(4)
        flo = lf.Flow("test")
        flo.add(a, b)
        inner_flo = uf.Flow("test2")
        inner_flo.add(c, d)
        flo.add(inner_flo)

        g = _replicate_graph_with_names(
            compiler.PatternCompiler(flo).compile())
        self.assertEqual(8, len(g))

        sub_g = g.subgraph(['a', 'b'])
        self.assertFalse(sub_g.has_edge('b', 'a'))
        self.assertTrue(sub_g.has_edge('a', 'b'))
        self.assertEqual({'invariant': True}, sub_g.get_edge_data("a", "b"))

        sub_g = g.subgraph(['c', 'd'])
        self.assertEqual(0, sub_g.number_of_edges())

        # This ensures that c and d do not start executing until after b.
        self.assertTrue(g.has_edge('b', 'test2'))
        self.assertTrue(g.has_edge('test2', 'c'))
        self.assertTrue(g.has_edge('test2', 'd'))

    def test_unordered_nested(self):
        a, b, c, d = test_utils.make_many(4)
        flo = uf.Flow("test")
        flo.add(a, b)
        flo2 = lf.Flow("test2")
        flo2.add(c, d)
        flo.add(flo2)

        g = _replicate_graph_with_names(
            compiler.PatternCompiler(flo).compile())
        self.assertEqual(8, len(g))
        self.assertCountEqual(g.edges(), [
            ('test', 'a'),
            ('test', 'b'),
            ('test', 'test2'),
            ('test2', 'c'),
            ('c', 'd'),
            ('d', 'test2[$]'),
            ('test2[$]', 'test[$]'),
            ('a', 'test[$]'),
            ('b', 'test[$]'),
        ])

    def test_unordered_nested_in_linear(self):
        a, b, c, d = test_utils.make_many(4)
        inner_flo = uf.Flow('ut').add(b, c)
        flo = lf.Flow('lt').add(a, inner_flo, d)

        g = _replicate_graph_with_names(
            compiler.PatternCompiler(flo).compile())
        self.assertEqual(8, len(g))
        self.assertCountEqual(g.edges(), [
            ('lt', 'a'),
            ('a', 'ut'),
            ('ut', 'b'),
            ('ut', 'c'),
            ('b', 'ut[$]'),
            ('c', 'ut[$]'),
            ('ut[$]', 'd'),
            ('d', 'lt[$]'),
        ])

    def test_graph(self):
        a, b, c, d = test_utils.make_many(4)
        flo = gf.Flow("test")
        flo.add(a, b, c, d)
        compilation = compiler.PatternCompiler(flo).compile()
        self.assertEqual(6, len(compilation.execution_graph))
        self.assertEqual(8, compilation.execution_graph.number_of_edges())

    def test_graph_nested(self):
        a, b, c, d, e, f, g = test_utils.make_many(7)
        flo = gf.Flow("test")
        flo.add(a, b, c, d)

        flo2 = lf.Flow('test2')
        flo2.add(e, f, g)
        flo.add(flo2)

        g = _replicate_graph_with_names(
            compiler.PatternCompiler(flo).compile())
        self.assertEqual(11, len(g))
        self.assertCountEqual(g.edges(), [
            ('test', 'a'),
            ('test', 'b'),
            ('test', 'c'),
            ('test', 'd'),
            ('a', 'test[$]'),
            ('b', 'test[$]'),
            ('c', 'test[$]'),
            ('d', 'test[$]'),

            ('test', 'test2'),
            ('test2', 'e'),
            ('e', 'f'),
            ('f', 'g'),

            ('g', 'test2[$]'),
            ('test2[$]', 'test[$]'),
        ])

    def test_graph_nested_graph(self):
        a, b, c, d, e, f, g = test_utils.make_many(7)
        flo = gf.Flow("test")
        flo.add(a, b, c, d)

        flo2 = gf.Flow('test2')
        flo2.add(e, f, g)
        flo.add(flo2)

        g = _replicate_graph_with_names(
            compiler.PatternCompiler(flo).compile())
        self.assertEqual(11, len(g))
        self.assertCountEqual(g.edges(), [
            ('test', 'a'),
            ('test', 'b'),
            ('test', 'c'),
            ('test', 'd'),
            ('test', 'test2'),

            ('test2', 'e'),
            ('test2', 'f'),
            ('test2', 'g'),

            ('e', 'test2[$]'),
            ('f', 'test2[$]'),
            ('g', 'test2[$]'),

            ('test2[$]', 'test[$]'),
            ('a', 'test[$]'),
            ('b', 'test[$]'),
            ('c', 'test[$]'),
            ('d', 'test[$]'),
        ])

    def test_graph_links(self):
        a, b, c, d = test_utils.make_many(4)
        flo = gf.Flow("test")
        flo.add(a, b, c, d)
        flo.link(a, b)
        flo.link(b, c)
        flo.link(c, d)

        g = _replicate_graph_with_names(
            compiler.PatternCompiler(flo).compile())
        self.assertEqual(6, len(g))
        self.assertCountEqual(g.edges(data=True), [
            ('test', 'a', {'invariant': True}),
            ('a', 'b', {'manual': True}),
            ('b', 'c', {'manual': True}),
            ('c', 'd', {'manual': True}),
            ('d', 'test[$]', {'invariant': True}),
        ])
        self.assertCountEqual(['test'], g.no_predecessors_iter())
        self.assertCountEqual(['test[$]'], g.no_successors_iter())

    def test_graph_dependencies(self):
        a = test_utils.ProvidesRequiresTask('a', provides=['x'], requires=[])
        b = test_utils.ProvidesRequiresTask('b', provides=[], requires=['x'])
        flo = gf.Flow("test").add(a, b)

        g = _replicate_graph_with_names(
            compiler.PatternCompiler(flo).compile())
        self.assertEqual(4, len(g))
        self.assertCountEqual(g.edges(data=True), [
            ('test', 'a', {'invariant': True}),
            ('a', 'b', {'reasons': set(['x'])}),
            ('b', 'test[$]', {'invariant': True}),
        ])
        self.assertCountEqual(['test'], g.no_predecessors_iter())
        self.assertCountEqual(['test[$]'], g.no_successors_iter())

    def test_graph_nested_requires(self):
        a = test_utils.ProvidesRequiresTask('a', provides=['x'], requires=[])
        b = test_utils.ProvidesRequiresTask('b', provides=[], requires=[])
        c = test_utils.ProvidesRequiresTask('c', provides=[], requires=['x'])
        inner_flo = lf.Flow("test2").add(b, c)
        flo = gf.Flow("test").add(a, inner_flo)

        g = _replicate_graph_with_names(
            compiler.PatternCompiler(flo).compile())
        self.assertEqual(7, len(g))
        self.assertCountEqual(g.edges(data=True), [
            ('test', 'a', {'invariant': True}),
            ('test2', 'b', {'invariant': True}),
            ('a', 'test2', {'reasons': set(['x'])}),
            ('b', 'c', {'invariant': True}),
            ('c', 'test2[$]', {'invariant': True}),
            ('test2[$]', 'test[$]', {'invariant': True}),
        ])
        self.assertCountEqual(['test'], list(g.no_predecessors_iter()))
        self.assertCountEqual(['test[$]'], list(g.no_successors_iter()))

    def test_graph_nested_provides(self):
        a = test_utils.ProvidesRequiresTask('a', provides=[], requires=['x'])
        b = test_utils.ProvidesRequiresTask('b', provides=['x'], requires=[])
        c = test_utils.ProvidesRequiresTask('c', provides=[], requires=[])
        inner_flo = lf.Flow("test2").add(b, c)
        flo = gf.Flow("test").add(a, inner_flo)

        g = _replicate_graph_with_names(
            compiler.PatternCompiler(flo).compile())
        self.assertEqual(7, len(g))
        self.assertCountEqual(g.edges(data=True), [
            ('test', 'test2', {'invariant': True}),
            ('a', 'test[$]', {'invariant': True}),

            # The 'x' requirement is produced out of test2...
            ('test2[$]', 'a', {'reasons': set(['x'])}),

            ('test2', 'b', {'invariant': True}),
            ('b', 'c', {'invariant': True}),
            ('c', 'test2[$]', {'invariant': True}),
        ])
        self.assertCountEqual(['test'], g.no_predecessors_iter())
        self.assertCountEqual(['test[$]'], g.no_successors_iter())

    def test_empty_flow_in_linear_flow(self):
        flo = lf.Flow('lf')
        a = test_utils.ProvidesRequiresTask('a', provides=[], requires=[])
        b = test_utils.ProvidesRequiresTask('b', provides=[], requires=[])
        empty_flo = gf.Flow("empty")
        flo.add(a, empty_flo, b)

        g = _replicate_graph_with_names(
            compiler.PatternCompiler(flo).compile())
        self.assertCountEqual(g.edges(), [
            ("lf", "a"),
            ("a", "empty"),
            ("empty", "empty[$]"),
            ("empty[$]", "b"),
            ("b", "lf[$]"),
        ])

    def test_many_empty_in_graph_flow(self):
        flo = gf.Flow('root')

        a = test_utils.ProvidesRequiresTask('a', provides=[], requires=[])
        flo.add(a)

        b = lf.Flow('b')
        b_0 = test_utils.ProvidesRequiresTask('b.0', provides=[], requires=[])
        b_1 = lf.Flow('b.1')
        b_2 = lf.Flow('b.2')
        b_3 = test_utils.ProvidesRequiresTask('b.3', provides=[], requires=[])
        b.add(b_0, b_1, b_2, b_3)
        flo.add(b)

        c = lf.Flow('c')
        c_0 = lf.Flow('c.0')
        c_1 = lf.Flow('c.1')
        c_2 = lf.Flow('c.2')
        c.add(c_0, c_1, c_2)
        flo.add(c)

        d = test_utils.ProvidesRequiresTask('d', provides=[], requires=[])
        flo.add(d)

        flo.link(b, d)
        flo.link(a, d)
        flo.link(c, d)

        g = _replicate_graph_with_names(
            compiler.PatternCompiler(flo).compile())

        self.assertTrue(g.has_edge('root', 'a'))
        self.assertTrue(g.has_edge('root', 'b'))
        self.assertTrue(g.has_edge('root', 'c'))

        self.assertTrue(g.has_edge('b.0', 'b.1'))
        self.assertTrue(g.has_edge('b.1[$]', 'b.2'))
        self.assertTrue(g.has_edge('b.2[$]', 'b.3'))

        self.assertTrue(g.has_edge('c.0[$]', 'c.1'))
        self.assertTrue(g.has_edge('c.1[$]', 'c.2'))

        self.assertTrue(g.has_edge('a', 'd'))
        self.assertTrue(g.has_edge('b[$]', 'd'))
        self.assertTrue(g.has_edge('c[$]', 'd'))
        self.assertEqual(20, len(g))

    def test_empty_flow_in_nested_flow(self):
        flow = lf.Flow('lf')
        a = test_utils.ProvidesRequiresTask('a', provides=[], requires=[])
        b = test_utils.ProvidesRequiresTask('b', provides=[], requires=[])

        flow2 = lf.Flow("lf-2")
        c = test_utils.ProvidesRequiresTask('c', provides=[], requires=[])
        d = test_utils.ProvidesRequiresTask('d', provides=[], requires=[])
        empty_flow = gf.Flow("empty")
        flow2.add(c, empty_flow, d)
        flow.add(a, flow2, b)

        g = _replicate_graph_with_names(
            compiler.PatternCompiler(flow).compile())
        for u, v in [('lf', 'a'), ('a', 'lf-2'),
                     ('lf-2', 'c'), ('c', 'empty'),
                     ('empty[$]', 'd'), ('d', 'lf-2[$]'),
                     ('lf-2[$]', 'b'), ('b', 'lf[$]')]:
            self.assertTrue(g.has_edge(u, v))

    def test_empty_flow_in_graph_flow(self):
        flow = lf.Flow('lf')
        a = test_utils.ProvidesRequiresTask('a', provides=['a'], requires=[])
        b = test_utils.ProvidesRequiresTask('b', provides=[], requires=['a'])
        empty_flow = lf.Flow("empty")
        flow.add(a, empty_flow, b)

        compilation = compiler.PatternCompiler(flow).compile()
        g = compilation.execution_graph
        self.assertTrue(g.has_edge(flow, a))
        self.assertTrue(g.has_edge(a, empty_flow))

        empty_flow_successors = list(g.successors(empty_flow))
        self.assertEqual(1, len(empty_flow_successors))
        empty_flow_terminal = empty_flow_successors[0]
        self.assertIs(empty_flow, empty_flow_terminal.flow)
        self.assertEqual(compiler.FLOW_END,
                         g.nodes[empty_flow_terminal]['kind'])
        self.assertTrue(g.has_edge(empty_flow_terminal, b))

    def test_empty_flow_in_graph_flow_linkage(self):
        flow = gf.Flow('lf')
        a = test_utils.ProvidesRequiresTask('a', provides=[], requires=[])
        b = test_utils.ProvidesRequiresTask('b', provides=[], requires=[])
        empty_flow = lf.Flow("empty")
        flow.add(a, empty_flow, b)
        flow.link(a, b)

        compilation = compiler.PatternCompiler(flow).compile()
        g = compilation.execution_graph
        self.assertTrue(g.has_edge(a, b))
        self.assertTrue(g.has_edge(flow, a))
        self.assertTrue(g.has_edge(flow, empty_flow))

    def test_checks_for_dups(self):
        flo = gf.Flow("test").add(
            test_utils.DummyTask(name="a"),
            test_utils.DummyTask(name="a")
        )
        e = engines.load(flo)
        self.assertRaisesRegex(exc.Duplicate,
                               '^Atoms with duplicate names',
                               e.compile)

    def test_checks_for_dups_globally(self):
        flo = gf.Flow("test").add(
            gf.Flow("int1").add(test_utils.DummyTask(name="a")),
            gf.Flow("int2").add(test_utils.DummyTask(name="a")))
        e = engines.load(flo)
        self.assertRaisesRegex(exc.Duplicate,
                               '^Atoms with duplicate names',
                               e.compile)

    def test_retry_in_linear_flow(self):
        flo = lf.Flow("test", retry.AlwaysRevert("c"))
        compilation = compiler.PatternCompiler(flo).compile()
        self.assertEqual(3, len(compilation.execution_graph))
        self.assertEqual(2, compilation.execution_graph.number_of_edges())

    def test_retry_in_unordered_flow(self):
        flo = uf.Flow("test", retry.AlwaysRevert("c"))
        compilation = compiler.PatternCompiler(flo).compile()
        self.assertEqual(3, len(compilation.execution_graph))
        self.assertEqual(2, compilation.execution_graph.number_of_edges())

    def test_retry_in_graph_flow(self):
        flo = gf.Flow("test", retry.AlwaysRevert("c"))
        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
        self.assertEqual(3, len(g))
        self.assertEqual(2, g.number_of_edges())

    def test_retry_in_nested_flows(self):
        c1 = retry.AlwaysRevert("c1")
        c2 = retry.AlwaysRevert("c2")
        inner_flo = lf.Flow("test2", c2)
        flo = lf.Flow("test", c1).add(inner_flo)

        g = _replicate_graph_with_names(
            compiler.PatternCompiler(flo).compile())
        self.assertEqual(6, len(g))
        self.assertCountEqual(g.edges(data=True), [
            ('test', 'c1', {'invariant': True}),
            ('c1', 'test2', {'invariant': True, 'retry': True}),
            ('test2', 'c2', {'invariant': True}),
            ('c2', 'test2[$]', {'invariant': True}),
            ('test2[$]', 'test[$]', {'invariant': True}),
        ])
        self.assertIs(c1, g.nodes['c2']['retry'])
        self.assertCountEqual(['test'], list(g.no_predecessors_iter()))
        self.assertCountEqual(['test[$]'], list(g.no_successors_iter()))

    def test_retry_in_linear_flow_with_tasks(self):
        c = retry.AlwaysRevert("c")
        a, b = test_utils.make_many(2)
        flo = lf.Flow("test", c).add(a, b)

        g = _replicate_graph_with_names(
            compiler.PatternCompiler(flo).compile())
        self.assertEqual(5, len(g))
        self.assertCountEqual(g.edges(data=True), [
            ('test', 'c', {'invariant': True}),
            ('a', 'b', {'invariant': True}),
            ('c', 'a', {'invariant': True, 'retry': True}),
            ('b', 'test[$]', {'invariant': True}),
        ])

        self.assertCountEqual(['test'], g.no_predecessors_iter())
        self.assertCountEqual(['test[$]'], g.no_successors_iter())
        self.assertIs(c, g.nodes['a']['retry'])
        self.assertIs(c, g.nodes['b']['retry'])

    def test_retry_in_unordered_flow_with_tasks(self):
        c = retry.AlwaysRevert("c")
        a, b = test_utils.make_many(2)
        flo = uf.Flow("test", c).add(a, b)

        g = _replicate_graph_with_names(
            compiler.PatternCompiler(flo).compile())
        self.assertEqual(5, len(g))
        self.assertCountEqual(g.edges(data=True), [
            ('test', 'c', {'invariant': True}),
            ('c', 'a', {'invariant': True, 'retry': True}),
            ('c', 'b', {'invariant': True, 'retry': True}),
            ('b', 'test[$]', {'invariant': True}),
            ('a', 'test[$]', {'invariant': True}),
        ])

        self.assertCountEqual(['test'], list(g.no_predecessors_iter()))
        self.assertCountEqual(['test[$]'], list(g.no_successors_iter()))
        self.assertIs(c, g.nodes['a']['retry'])
        self.assertIs(c, g.nodes['b']['retry'])

    def test_retry_in_graph_flow_with_tasks(self):
        r = retry.AlwaysRevert("r")
        a, b, c = test_utils.make_many(3)
        flo = gf.Flow("test", r).add(a, b, c).link(b, c)

        g = _replicate_graph_with_names(
            compiler.PatternCompiler(flo).compile())
        self.assertCountEqual(g.edges(data=True), [
            ('test', 'r', {'invariant': True}),
            ('r', 'a', {'invariant': True, 'retry': True}),
            ('r', 'b', {'invariant': True, 'retry': True}),
            ('b', 'c', {'manual': True}),
            ('a', 'test[$]', {'invariant': True}),
            ('c', 'test[$]', {'invariant': True}),
        ])

        self.assertCountEqual(['test'], g.no_predecessors_iter())
        self.assertCountEqual(['test[$]'], g.no_successors_iter())
        self.assertIs(r, g.nodes['a']['retry'])
        self.assertIs(r, g.nodes['b']['retry'])
        self.assertIs(r, g.nodes['c']['retry'])

    def test_retries_hierarchy(self):
        c1 = retry.AlwaysRevert("c1")
        c2 = retry.AlwaysRevert("c2")
        a, b, c, d = test_utils.make_many(4)
        inner_flo = lf.Flow("test2", c2).add(b, c)
        flo = lf.Flow("test", c1).add(a, inner_flo, d)

        g = _replicate_graph_with_names(
            compiler.PatternCompiler(flo).compile())
        self.assertEqual(10, len(g))
        self.assertCountEqual(g.edges(data=True), [
            ('test', 'c1', {'invariant': True}),
            ('c1', 'a', {'invariant': True, 'retry': True}),
            ('a', 'test2', {'invariant': True}),
            ('test2', 'c2', {'invariant': True}),
            ('c2', 'b', {'invariant': True, 'retry': True}),
            ('b', 'c', {'invariant': True}),
            ('c', 'test2[$]', {'invariant': True}),
            ('test2[$]', 'd', {'invariant': True}),
            ('d', 'test[$]', {'invariant': True}),
        ])
        self.assertIs(c1, g.nodes['a']['retry'])
        self.assertIs(c1, g.nodes['d']['retry'])
        self.assertIs(c2, g.nodes['b']['retry'])
        self.assertIs(c2, g.nodes['c']['retry'])
        self.assertIs(c1, g.nodes['c2']['retry'])
        self.assertIsNone(g.nodes['c1'].get('retry'))

    def test_retry_subflows_hierarchy(self):
        c1 = retry.AlwaysRevert("c1")
        a, b, c, d = test_utils.make_many(4)
        inner_flo = lf.Flow("test2").add(b, c)
        flo = lf.Flow("test", c1).add(a, inner_flo, d)

        g = _replicate_graph_with_names(
            compiler.PatternCompiler(flo).compile())
        self.assertEqual(9, len(g))
        self.assertCountEqual(g.edges(data=True), [
            ('test', 'c1', {'invariant': True}),
            ('c1', 'a', {'invariant': True, 'retry': True}),
            ('a', 'test2', {'invariant': True}),
            ('test2', 'b', {'invariant': True}),
            ('b', 'c', {'invariant': True}),
            ('c', 'test2[$]', {'invariant': True}),
            ('test2[$]', 'd', {'invariant': True}),
            ('d', 'test[$]', {'invariant': True}),
        ])
        self.assertIs(c1, g.nodes['a']['retry'])
        self.assertIs(c1, g.nodes['d']['retry'])
        self.assertIs(c1, g.nodes['b']['retry'])
        self.assertIs(c1, g.nodes['c']['retry'])
        self.assertIsNone(g.nodes['c1'].get('retry'))
