import unittest

from dbtp.directedgraph import DirectedGraph, CyclicGraphError, Edge, Vertex

class DirectedGraphTest(unittest.TestCase):
    def test_add_vertex_and_vertex_count(self):
        g = DirectedGraph()
        a = Vertex(0, "a")
        g.add_vertex(a)
        self.assertEqual(g.vertex_count(), 1)

    def test_add_edge_and_edge_count(self):
        g = DirectedGraph()
        a, b = Vertex(0, "a"), Vertex(1, "b")
        e = Edge(a.id, b.id, "ab")
        g.add_vertex(a)
        g.add_vertex(b)
        g.add_edge(e)
        self.assertEqual(g.vertex_count(), 2)
        self.assertEqual(g.edge_count(), 1)
        self.assertEqual(g.adjacency[a.id], [b.id])

    def test_get_out_degree(self):
        g = DirectedGraph()
        a, b, c = Vertex(0, "a"), Vertex(1, "b"), Vertex(2, "c")
        g.add_vertex(a)
        g.add_vertex(b)
        g.add_vertex(c)
        g.add_edge(Edge(a.id, b.id))
        g.add_edge(Edge(a.id, c.id))
        outdeg = g.get_out_degree()
        self.assertEqual(outdeg[a.id], 2)
        self.assertEqual(outdeg[b.id], 0)
        self.assertEqual(outdeg[c.id], 0)

    def test_get_in_degree(self):
        g = DirectedGraph()
        a, b, c = Vertex(0, "a"), Vertex(1, "b"), Vertex(2, "c")
        g.add_vertex(a)
        g.add_vertex(b)
        g.add_vertex(c)
        g.add_edge(Edge(a.id, b.id))
        g.add_edge(Edge(c.id, b.id))
        indeg = g.get_in_degree()
        self.assertEqual(indeg[a.id], 0)
        self.assertEqual(indeg[b.id], 2)
        self.assertEqual(indeg[c.id], 0)

    def test_topological_sort_linear(self):
        g = DirectedGraph()
        a, b, c = Vertex(0, "a"), Vertex(1, "b"), Vertex(2, "c")
        for v in (a, b, c):
            g.add_vertex(v)
        g.add_edge(Edge(a.id, b.id))
        g.add_edge(Edge(b.id, c.id))
        topo = g.topological_sort()
        self.assertEqual(topo, [a.id, b.id, c.id])

    def test_topological_sort_multiple_sources(self):
        g = DirectedGraph()
        a, b, c = Vertex(0, "a"), Vertex(1, "b"), Vertex(2, "c")
        for v in (a, b, c):
            g.add_vertex(v)
        g.add_edge(Edge(a.id, c.id))
        g.add_edge(Edge(b.id, c.id))
        topo = g.topological_sort()
        self.assertEqual(topo, [a.id, b.id, c.id])

    def test_topological_sort_cycle_raises(self):
        g = DirectedGraph()
        a, b, c = Vertex(0, "a"), Vertex(1, "b"), Vertex(2, "c")
        for v in (a, b, c):
            g.add_vertex(v)
        g.add_edge(Edge(a.id, b.id))
        g.add_edge(Edge(b.id, c.id))
        g.add_edge(Edge(c.id, a.id))
        with self.assertRaises(CyclicGraphError):
            g.topological_sort()

    if __name__ == "__main__":
        unittest.main()

if __name__ == "__main__":
    unittest.main()