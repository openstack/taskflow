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

from taskflow.engines.action_engine import compiler
from taskflow import exceptions as exc
from taskflow.patterns import graph_flow as gf
from taskflow.patterns import linear_flow as lf
from taskflow.patterns import unordered_flow as uf
from taskflow import retry
from taskflow import test
from taskflow.tests import utils as test_utils


class PatternCompileTest(test.TestCase):
    def test_task(self):
        task = test_utils.DummyTask(name='a')
        compilation = compiler.PatternCompiler(task).compile()
        g = compilation.execution_graph
        self.assertEqual(list(g.nodes()), [task])
        self.assertEqual(list(g.edges()), [])

    def test_retry(self):
        r = retry.AlwaysRevert('r1')
        self.assertRaises(TypeError, compiler.PatternCompiler(r).compile)

    def test_wrong_object(self):
        msg_regex = '^Unknown object .* requested to compile'
        self.assertRaisesRegexp(TypeError, msg_regex,
                                compiler.PatternCompiler(42).compile)

    def test_empty(self):
        flo = lf.Flow("test")
        self.assertRaises(exc.Empty, compiler.PatternCompiler(flo).compile)

    def test_linear(self):
        a, b, c, d = test_utils.make_many(4)
        flo = lf.Flow("test")
        flo.add(a, b, c)
        inner_flo = lf.Flow("sub-test")
        inner_flo.add(d)
        flo.add(inner_flo)

        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
        self.assertEqual(6, len(g))

        order = g.topological_sort()
        self.assertEqual([flo, a, b, c, inner_flo, d], order)
        self.assertTrue(g.has_edge(c, inner_flo))
        self.assertTrue(g.has_edge(inner_flo, d))
        self.assertEqual(g.get_edge_data(inner_flo, d), {'invariant': True})

        self.assertEqual([d], list(g.no_successors_iter()))
        self.assertEqual([flo], list(g.no_predecessors_iter()))

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
        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
        self.assertEqual(5, len(g))
        self.assertItemsEqual(g.edges(), [
            (flo, a),
            (flo, b),
            (flo, c),
            (flo, d),
        ])
        self.assertEqual(set([a, b, c, d]),
                         set(g.no_successors_iter()))
        self.assertEqual(set([flo]),
                         set(g.no_predecessors_iter()))

    def test_linear_nested(self):
        a, b, c, d = test_utils.make_many(4)
        flo = lf.Flow("test")
        flo.add(a, b)
        inner_flo = uf.Flow("test2")
        inner_flo.add(c, d)
        flo.add(inner_flo)

        compilation = compiler.PatternCompiler(flo).compile()
        graph = compilation.execution_graph
        self.assertEqual(6, len(graph))

        lb = graph.subgraph([a, b])
        self.assertFalse(lb.has_edge(b, a))
        self.assertTrue(lb.has_edge(a, b))
        self.assertEqual(graph.get_edge_data(a, b), {'invariant': True})

        ub = graph.subgraph([c, d])
        self.assertEqual(0, ub.number_of_edges())

        # This ensures that c and d do not start executing until after b.
        self.assertTrue(graph.has_edge(b, inner_flo))
        self.assertTrue(graph.has_edge(inner_flo, c))
        self.assertTrue(graph.has_edge(inner_flo, d))

    def test_unordered_nested(self):
        a, b, c, d = test_utils.make_many(4)
        flo = uf.Flow("test")
        flo.add(a, b)
        flo2 = lf.Flow("test2")
        flo2.add(c, d)
        flo.add(flo2)

        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
        self.assertEqual(6, len(g))
        self.assertItemsEqual(g.edges(), [
            (flo, a),
            (flo, b),
            (flo, flo2),
            (flo2, c),
            (c, d)
        ])

    def test_unordered_nested_in_linear(self):
        a, b, c, d = test_utils.make_many(4)
        inner_flo = uf.Flow('ut').add(b, c)
        flo = lf.Flow('lt').add(a, inner_flo, d)

        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
        self.assertEqual(6, len(g))
        self.assertItemsEqual(g.edges(), [
            (flo, a),
            (a, inner_flo),
            (inner_flo, b),
            (inner_flo, c),
            (b, d),
            (c, d),
        ])

    def test_graph(self):
        a, b, c, d = test_utils.make_many(4)
        flo = gf.Flow("test")
        flo.add(a, b, c, d)

        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
        self.assertEqual(5, len(g))
        self.assertEqual(4, g.number_of_edges())

    def test_graph_nested(self):
        a, b, c, d, e, f, g = test_utils.make_many(7)
        flo = gf.Flow("test")
        flo.add(a, b, c, d)

        flo2 = lf.Flow('test2')
        flo2.add(e, f, g)
        flo.add(flo2)

        compilation = compiler.PatternCompiler(flo).compile()
        graph = compilation.execution_graph
        self.assertEqual(9, len(graph))
        self.assertItemsEqual(graph.edges(), [
            (flo, a),
            (flo, b),
            (flo, c),
            (flo, d),
            (flo, flo2),

            (flo2, e),
            (e, f),
            (f, g),
        ])

    def test_graph_nested_graph(self):
        a, b, c, d, e, f, g = test_utils.make_many(7)
        flo = gf.Flow("test")
        flo.add(a, b, c, d)

        flo2 = gf.Flow('test2')
        flo2.add(e, f, g)
        flo.add(flo2)

        compilation = compiler.PatternCompiler(flo).compile()
        graph = compilation.execution_graph
        self.assertEqual(9, len(graph))
        self.assertItemsEqual(graph.edges(), [
            (flo, a),
            (flo, b),
            (flo, c),
            (flo, d),
            (flo, flo2),

            (flo2, e),
            (flo2, f),
            (flo2, g),
        ])

    def test_graph_links(self):
        a, b, c, d = test_utils.make_many(4)
        flo = gf.Flow("test")
        flo.add(a, b, c, d)
        flo.link(a, b)
        flo.link(b, c)
        flo.link(c, d)

        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
        self.assertEqual(5, len(g))
        self.assertItemsEqual(g.edges(data=True), [
            (flo, a, {'invariant': True}),

            (a, b, {'manual': True}),
            (b, c, {'manual': True}),
            (c, d, {'manual': True}),
        ])
        self.assertItemsEqual([flo], g.no_predecessors_iter())
        self.assertItemsEqual([d], g.no_successors_iter())

    def test_graph_dependencies(self):
        a = test_utils.ProvidesRequiresTask('a', provides=['x'], requires=[])
        b = test_utils.ProvidesRequiresTask('b', provides=[], requires=['x'])
        flo = gf.Flow("test").add(a, b)

        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
        self.assertEqual(3, len(g))
        self.assertItemsEqual(g.edges(data=True), [
            (flo, a, {'invariant': True}),
            (a, b, {'reasons': set(['x'])})
        ])
        self.assertItemsEqual([flo], g.no_predecessors_iter())
        self.assertItemsEqual([b], g.no_successors_iter())

    def test_graph_nested_requires(self):
        a = test_utils.ProvidesRequiresTask('a', provides=['x'], requires=[])
        b = test_utils.ProvidesRequiresTask('b', provides=[], requires=[])
        c = test_utils.ProvidesRequiresTask('c', provides=[], requires=['x'])
        inner_flo = lf.Flow("test2").add(b, c)
        flo = gf.Flow("test").add(a, inner_flo)

        compilation = compiler.PatternCompiler(flo).compile()
        graph = compilation.execution_graph
        self.assertEqual(5, len(graph))
        self.assertItemsEqual(graph.edges(data=True), [
            (flo, a, {'invariant': True}),
            (inner_flo, b, {'invariant': True}),
            (a, inner_flo, {'reasons': set(['x'])}),
            (b, c, {'invariant': True}),
        ])
        self.assertItemsEqual([flo], graph.no_predecessors_iter())
        self.assertItemsEqual([c], graph.no_successors_iter())

    def test_graph_nested_provides(self):
        a = test_utils.ProvidesRequiresTask('a', provides=[], requires=['x'])
        b = test_utils.ProvidesRequiresTask('b', provides=['x'], requires=[])
        c = test_utils.ProvidesRequiresTask('c', provides=[], requires=[])
        inner_flo = lf.Flow("test2").add(b, c)
        flo = gf.Flow("test").add(a, inner_flo)

        compilation = compiler.PatternCompiler(flo).compile()
        graph = compilation.execution_graph
        self.assertEqual(5, len(graph))
        self.assertItemsEqual(graph.edges(data=True), [
            (flo, inner_flo, {'invariant': True}),

            (inner_flo, b, {'invariant': True}),
            (b, c, {'invariant': True}),
            (c, a, {'reasons': set(['x'])}),
        ])
        self.assertItemsEqual([flo], graph.no_predecessors_iter())
        self.assertItemsEqual([a], graph.no_successors_iter())

    def test_empty_flow_in_linear_flow(self):
        flo = lf.Flow('lf')
        a = test_utils.ProvidesRequiresTask('a', provides=[], requires=[])
        b = test_utils.ProvidesRequiresTask('b', provides=[], requires=[])
        empty_flo = gf.Flow("empty")
        flo.add(a, empty_flo, b)

        compilation = compiler.PatternCompiler(flo).compile()
        graph = compilation.execution_graph
        self.assertItemsEqual(graph.edges(), [
            (flo, a),
            (a, empty_flo),
            (empty_flo, b),
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

        compilation = compiler.PatternCompiler(flo).compile()
        graph = compilation.execution_graph

        self.assertTrue(graph.has_edge(flo, a))

        self.assertTrue(graph.has_edge(flo, b))
        self.assertTrue(graph.has_edge(b_0, b_1))
        self.assertTrue(graph.has_edge(b_1, b_2))
        self.assertTrue(graph.has_edge(b_2, b_3))

        self.assertTrue(graph.has_edge(flo, c))
        self.assertTrue(graph.has_edge(c_0, c_1))
        self.assertTrue(graph.has_edge(c_1, c_2))

        self.assertTrue(graph.has_edge(b_3, d))
        self.assertEqual(12, len(graph))

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

        compilation = compiler.PatternCompiler(flow).compile()
        g = compilation.execution_graph

        for source, target in [(flow, a), (a, flow2),
                               (flow2, c), (c, empty_flow),
                               (empty_flow, d), (d, b)]:
            self.assertTrue(g.has_edge(source, target))

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
        self.assertTrue(g.has_edge(empty_flow, b))

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
        self.assertRaisesRegexp(exc.Duplicate,
                                '^Atoms with duplicate names',
                                compiler.PatternCompiler(flo).compile)

    def test_checks_for_dups_globally(self):
        flo = gf.Flow("test").add(
            gf.Flow("int1").add(test_utils.DummyTask(name="a")),
            gf.Flow("int2").add(test_utils.DummyTask(name="a")))
        self.assertRaisesRegexp(exc.Duplicate,
                                '^Atoms with duplicate names',
                                compiler.PatternCompiler(flo).compile)

    def test_retry_in_linear_flow(self):
        flo = lf.Flow("test", retry.AlwaysRevert("c"))
        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
        self.assertEqual(2, len(g))
        self.assertEqual(1, g.number_of_edges())

    def test_retry_in_unordered_flow(self):
        flo = uf.Flow("test", retry.AlwaysRevert("c"))
        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
        self.assertEqual(2, len(g))
        self.assertEqual(1, g.number_of_edges())

    def test_retry_in_graph_flow(self):
        flo = gf.Flow("test", retry.AlwaysRevert("c"))
        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
        self.assertEqual(2, len(g))
        self.assertEqual(1, g.number_of_edges())

    def test_retry_in_nested_flows(self):
        c1 = retry.AlwaysRevert("c1")
        c2 = retry.AlwaysRevert("c2")
        inner_flo = lf.Flow("test2", c2)
        flo = lf.Flow("test", c1).add(inner_flo)
        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph

        self.assertEqual(4, len(g))
        self.assertItemsEqual(g.edges(data=True), [
            (flo, c1, {'invariant': True}),
            (c1, inner_flo, {'invariant': True, 'retry': True}),
            (inner_flo, c2, {'invariant': True}),
        ])
        self.assertIs(c1, g.node[c2]['retry'])
        self.assertItemsEqual([flo], g.no_predecessors_iter())
        self.assertItemsEqual([c2], g.no_successors_iter())

    def test_retry_in_linear_flow_with_tasks(self):
        c = retry.AlwaysRevert("c")
        a, b = test_utils.make_many(2)
        flo = lf.Flow("test", c).add(a, b)
        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph

        self.assertEqual(4, len(g))
        self.assertItemsEqual(g.edges(data=True), [
            (flo, c, {'invariant': True}),
            (a, b, {'invariant': True}),
            (c, a, {'invariant': True, 'retry': True})
        ])

        self.assertItemsEqual([flo], g.no_predecessors_iter())
        self.assertItemsEqual([b], g.no_successors_iter())
        self.assertIs(c, g.node[a]['retry'])
        self.assertIs(c, g.node[b]['retry'])

    def test_retry_in_unordered_flow_with_tasks(self):
        c = retry.AlwaysRevert("c")
        a, b = test_utils.make_many(2)
        flo = uf.Flow("test", c).add(a, b)
        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph

        self.assertEqual(4, len(g))
        self.assertItemsEqual(g.edges(data=True), [
            (flo, c, {'invariant': True}),
            (c, a, {'invariant': True, 'retry': True}),
            (c, b, {'invariant': True, 'retry': True}),
        ])

        self.assertItemsEqual([flo], g.no_predecessors_iter())
        self.assertItemsEqual([a, b], g.no_successors_iter())
        self.assertIs(c, g.node[a]['retry'])
        self.assertIs(c, g.node[b]['retry'])

    def test_retry_in_graph_flow_with_tasks(self):
        r = retry.AlwaysRevert("cp")
        a, b, c = test_utils.make_many(3)
        flo = gf.Flow("test", r).add(a, b, c).link(b, c)
        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
        self.assertEqual(5, len(g))

        self.assertItemsEqual(g.edges(data=True), [
            (flo, r, {'invariant': True}),
            (r, a, {'invariant': True, 'retry': True}),
            (r, b, {'invariant': True, 'retry': True}),
            (b, c, {'manual': True})
        ])

        self.assertItemsEqual([flo], g.no_predecessors_iter())
        self.assertItemsEqual([a, c], g.no_successors_iter())
        self.assertIs(r, g.node[a]['retry'])
        self.assertIs(r, g.node[b]['retry'])
        self.assertIs(r, g.node[c]['retry'])

    def test_retries_hierarchy(self):
        c1 = retry.AlwaysRevert("cp1")
        c2 = retry.AlwaysRevert("cp2")
        a, b, c, d = test_utils.make_many(4)
        inner_flo = lf.Flow("test", c2).add(b, c)
        flo = lf.Flow("test", c1).add(a, inner_flo, d)
        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph

        self.assertEqual(8, len(g))
        self.assertItemsEqual(g.edges(data=True), [
            (flo, c1, {'invariant': True}),
            (c1, a, {'invariant': True, 'retry': True}),
            (a, inner_flo, {'invariant': True}),
            (inner_flo, c2, {'invariant': True}),
            (c2, b, {'invariant': True, 'retry': True}),
            (b, c, {'invariant': True}),
            (c, d, {'invariant': True}),
        ])
        self.assertIs(c1, g.node[a]['retry'])
        self.assertIs(c1, g.node[d]['retry'])
        self.assertIs(c2, g.node[b]['retry'])
        self.assertIs(c2, g.node[c]['retry'])
        self.assertIs(c1, g.node[c2]['retry'])
        self.assertIs(None, g.node[c1].get('retry'))

    def test_retry_subflows_hierarchy(self):
        c1 = retry.AlwaysRevert("cp1")
        a, b, c, d = test_utils.make_many(4)
        inner_flo = lf.Flow("test").add(b, c)
        flo = lf.Flow("test", c1).add(a, inner_flo, d)
        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph

        self.assertEqual(7, len(g))
        self.assertItemsEqual(g.edges(data=True), [
            (flo, c1, {'invariant': True}),
            (c1, a, {'invariant': True, 'retry': True}),
            (a, inner_flo, {'invariant': True}),
            (inner_flo, b, {'invariant': True}),
            (b, c, {'invariant': True}),
            (c, d, {'invariant': True}),
        ])
        self.assertIs(c1, g.node[a]['retry'])
        self.assertIs(c1, g.node[d]['retry'])
        self.assertIs(c1, g.node[b]['retry'])
        self.assertIs(c1, g.node[c]['retry'])
        self.assertIs(None, g.node[c1].get('retry'))
