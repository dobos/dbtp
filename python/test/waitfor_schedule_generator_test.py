import unittest
import random

from dbtp import (
    DirectedGraph,
    CyclicGraphError,
    Vertex,
    Edge,
    OperationType,
    WaitforScheduleGenerator,
    Schedule
)

class WaitforScheduleGeneratorTest(unittest.TestCase):

    def test_generate_random_wait_for_graph_acyclic(self):
        """Acyclic wait-for graph should guarantee no deadlock cycle."""

        generator = WaitforScheduleGenerator()
        graph = generator.generate_random_wait_for_graph(
            acyclic=True,
            cyclic=False,
        )

        self.assertEqual(len(graph.vertices), 4)
        self.assertEqual(len(graph.edges), 4)

        try:
            graph.topological_sort()
        except CyclicGraphError as e:
            self.fail(f"Generated wait-for graph is cyclic: {e}")

    def test_generate_random_wait_for_graph_cyclic(self):
        """Cyclic wait-for graph should guarantee deadlock cycle."""

        generator = WaitforScheduleGenerator()
        graph = generator.generate_random_wait_for_graph(
            acyclic=False,
            cyclic=True,
        )

        self.assertEqual(len(graph.vertices), 4)
        self.assertEqual(len(graph.edges), 4)

        with self.assertRaises(CyclicGraphError):
            graph.topological_sort()

    def test_generate_random_wait_for_graph_conflicting_options(self):
        generator = WaitforScheduleGenerator()
        with self.assertRaises(ValueError):
            generator.generate_random_wait_for_graph(
                acyclic=True,
                cyclic=True,
            )

    def test_generate_schedule_from_wait_for_graph_preserves_edges(self):
        generator = WaitforScheduleGenerator()
        vertices = [
            Vertex(id=1, label=1),
            Vertex(id=2, label=2),
            Vertex(id=3, label=3),
            Vertex(id=4, label=4),
        ]
        edges = [
            Edge(source=1, target=2),
            Edge(source=1, target=3),
            Edge(source=4, target=2),
            Edge(source=3, target=4),
        ]
        graph = DirectedGraph(vertices=vertices, edges=edges)

        schedule = generator.generate_schedule_from_wait_for_graph(graph)

        generated_wait_for = schedule.build_wait_for_graph()

        expected_edges = set((e.source, e.target) for e in edges)
        actual_edges = set((e.source, e.target) for e in generated_wait_for.edges.values())

        self.assertEqual(actual_edges, expected_edges)
        self.assertTrue(schedule.is_two_phase_locked())
        self.assertTrue(schedule.is_legal())
        self.assertFalse(any(op.op == OperationType.COMMIT for op in schedule.operations))

    def test_generate_schedule_from_wait_for_graph_all_transactions_do_something(self):
        vertices = [
            Vertex(id=1, label=1),
            Vertex(id=2, label=2),
            Vertex(id=3, label=3),
            Vertex(id=4, label=4),
        ]
        # Tx 4 is intentionally isolated in the wait-for graph.
        edges = [
            Edge(source=1, target=2),
            Edge(source=2, target=3),
        ]
        graph = DirectedGraph(vertices=vertices, edges=edges)

        generator = WaitforScheduleGenerator()
        schedule = generator.generate_schedule_from_wait_for_graph(graph)

        active_txs = {
            op.tx
            for op in schedule.operations
            if op.op in {
                OperationType.SLOCK,
                OperationType.XLOCK,
                OperationType.READ,
                OperationType.WRITE,
            }
        }

        self.assertEqual(active_txs, {1, 2, 3, 4})
        self.assertFalse(any(op.op == OperationType.COMMIT for op in schedule.operations))

    def test_generate_schedule_from_wait_for_graph_cyclic_deadlock(self):
        vertices = [
            Vertex(id=1, label=1),
            Vertex(id=2, label=2),
            Vertex(id=3, label=3),
        ]
        edges = [
            Edge(source=1, target=2),
            Edge(source=2, target=3),
            Edge(source=3, target=1),
        ]
        graph = DirectedGraph(vertices=vertices, edges=edges)

        generator = WaitforScheduleGenerator()
        schedule = generator.generate_schedule_from_wait_for_graph(graph)

        self.assertTrue(schedule.has_deadlock())
        self.assertTrue(schedule.is_two_phase_locked())
        self.assertTrue(schedule.is_legal())

    def test_generate_schedule_from_wait_for_graph_is_randomized(self):
        vertices = [
            Vertex(id=1, label=1),
            Vertex(id=2, label=2),
            Vertex(id=3, label=3),
            Vertex(id=4, label=4),
        ]
        edges = [
            Edge(source=1, target=2),
            Edge(source=2, target=3),
            Edge(source=3, target=4),
            Edge(source=4, target=1),
        ]
        graph = DirectedGraph(vertices=vertices, edges=edges)

        generator = WaitforScheduleGenerator()
        variants = set()
        for seed in range(10):
            random.seed(seed)
            schedule = generator.generate_schedule_from_wait_for_graph(graph)
            variants.add(str(schedule))

        # We expect multiple valid variants due to randomized event ordering/lock choices.
        self.assertGreater(len(variants), 1)

if __name__ == "__main__":
    unittest.main()
