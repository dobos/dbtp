import unittest

from dbtp import Graph

class GraphTest(unittest.TestCase):
    def test_graph_initialization(self):
        g = Graph()
        self.assertIsInstance(g, Graph)
