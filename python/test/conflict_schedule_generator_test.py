import unittest
import random

from dbtp import (
    DirectedGraph,
    CyclicGraphError,
    Vertex,
    Edge,
    OperationType,
    ConflictScheduleGenerator,
    Schedule
)

class ConflictScheduleGeneratorTest(unittest.TestCase):

    def assert_precedence_graph_matches(self, schedule: Schedule, graph: DirectedGraph):
        expected_edges = set(graph.edges.keys())
        actual_edges = set(schedule.build_precedence_graph().edges.keys())
        self.assertEqual(actual_edges, expected_edges)

    def assert_transaction_order_preserved(self, original: Schedule, candidate: Schedule):
        tx_ids = {op.tx for op in original.operations}
        for tx_id in tx_ids:
            original_ops = [
                (op.op, op.item)
                for op in original.operations
                if op.tx == tx_id
            ]
            candidate_ops = [
                (op.op, op.item)
                for op in candidate.operations
                if op.tx == tx_id
            ]
            self.assertEqual(candidate_ops, original_ops)
    
    def test_generate_random_precedence_graph(self):
        """Test generation of precedence graph from schedule"""

        generator = ConflictScheduleGenerator()
        graph = generator.generate_random_precedence_graph(
            acyclic = True,
            cyclic = False
        )
        
        # Should have 4 vertices and 4 edges
        self.assertEqual(len(graph.vertices), 4)
        self.assertEqual(len(graph.edges), 4)
        
        # Check if it's not cyclic
        try:
            topo_order = graph.topological_sort()
            pass
        except CyclicGraphError as e:
            self.fail(f"Generated graph is cyclic: {e}")

    def test_generate_random_cyclic_precedence_graph(self):
        """Test generation of precedence graph from schedule"""
        generator = ConflictScheduleGenerator()
        graph = generator.generate_random_precedence_graph(
            acyclic = False,
            cyclic = True
        )
        
        # Should have 4 vertices and 4 edges
        self.assertEqual(len(graph.vertices), 4)
        self.assertEqual(len(graph.edges), 4)

        # Check if it's cyclic
        with self.assertRaises(CyclicGraphError):
            topo_order = graph.topological_sort()

    def test_generate_random_cyclic_precedence_graph_avoids_two_node_cycles(self):
        generator = ConflictScheduleGenerator(avoid_two_node_cycles=True)

        for seed in range(10):
            random.seed(seed)
            graph = generator.generate_random_precedence_graph(
                transaction_count=5,
                edge_count=6,
                acyclic=False,
                cyclic=True
            )

            with self.assertRaises(CyclicGraphError):
                graph.topological_sort()

            for source, target in graph.edges.keys():
                self.assertNotIn((target, source), graph.edges)

    def test_generate_random_cyclic_precedence_graph_without_two_node_cycles_requires_three_nodes(self):
        generator = ConflictScheduleGenerator(avoid_two_node_cycles=True)

        with self.assertRaises(ValueError):
            generator.generate_random_precedence_graph(
                transaction_count=2,
                edge_count=2,
                acyclic=False,
                cyclic=True
            )

    def test_simple_two_transaction_chain(self):
        """Test T1 -> T2 precedence"""
        vertices = [
            Vertex(id=1, label=1),
            Vertex(id=2, label=2)
        ]
        edges = [
            Edge(source=1, target=2, label=None)
        ]
        
        graph = DirectedGraph(vertices=vertices, edges=edges)
        generator = ConflictScheduleGenerator()
        schedule = generator.generate_schedule_from_acyclic_precedence_graph(graph)
        
        # Should have 2 operations: W_1(A), R_2(A)
        self.assertEqual(len(schedule.operations), 2)
        self.assertEqual(schedule.operations[0].tx, 1)
        self.assertEqual(schedule.operations[0].op, OperationType.WRITE)
        self.assertEqual(schedule.operations[0].item, "A")
        
        self.assertEqual(schedule.operations[1].tx, 2)
        self.assertEqual(schedule.operations[1].op, OperationType.READ)
        self.assertEqual(schedule.operations[1].item, "A")

        self.assertEqual(str(schedule), "S_1 : W_1(A), R_2(A)")
    
    def test_three_transaction_chain(self):
        """Test T1 -> T2 -> T3 precedence"""
        vertices = [
            Vertex(id=1, label=1),
            Vertex(id=2, label=2),
            Vertex(id=3, label=3)
        ]
        edges = [
            Edge(source=1, target=2, label=None),
            Edge(source=2, target=3, label=None)
        ]
        
        graph = DirectedGraph(vertices=vertices, edges=edges)
        generator = ConflictScheduleGenerator()
        schedule = generator.generate_schedule_from_acyclic_precedence_graph(graph)
        
        # Should have 4 operations: W_1(A), R_2(A), W_2(B), R_3(B)
        self.assertEqual(len(schedule.operations), 4)
        
        # T1 writes X0
        self.assertEqual(schedule.operations[0].tx, 1)
        self.assertEqual(schedule.operations[0].op, OperationType.WRITE)
        
        # T2 reads X0 and writes X1
        self.assertEqual(schedule.operations[1].tx, 2)
        self.assertEqual(schedule.operations[1].op, OperationType.READ)
        self.assertEqual(schedule.operations[2].tx, 2)
        self.assertEqual(schedule.operations[2].op, OperationType.WRITE)
        
        # T3 reads X1
        self.assertEqual(schedule.operations[3].tx, 3)
        self.assertEqual(schedule.operations[3].op, OperationType.READ)

        self.assertEqual(str(schedule), "S_1 : W_1(A), R_2(A), W_2(B), R_3(B)")

    def test_item_reuse_when_graph_already_contains_induced_edges(self):
        """When all pairwise induced edges already exist, items can be safely reused."""
        vertices = [
            Vertex(id=1, label=1),
            Vertex(id=2, label=2),
            Vertex(id=3, label=3),
        ]
        edges = [
            Edge(source=1, target=2, label=None),
            Edge(source=1, target=3, label=None),
            Edge(source=2, target=3, label=None),
        ]

        graph = DirectedGraph(vertices=vertices, edges=edges)
        generator = ConflictScheduleGenerator()
        schedule = generator.generate_schedule_from_acyclic_precedence_graph(graph)

        self.assert_precedence_graph_matches(schedule, graph)

        write_items = [op.item for op in schedule.operations if op.op == OperationType.WRITE]
        self.assertLess(len(set(write_items)), len(edges))
    
    def test_diamond_graph(self):
        """Test diamond precedence: T1 -> T2, T1 -> T3, T2 -> T4, T3 -> T4"""
        vertices = [
            Vertex(id=1, label=1),
            Vertex(id=2, label=2),
            Vertex(id=3, label=3),
            Vertex(id=4, label=4)
        ]
        edges = [
            Edge(source=1, target=2, label=None),
            Edge(source=1, target=3, label=None),
            Edge(source=2, target=4, label=None),
            Edge(source=3, target=4, label=None)
        ]
        
        graph = DirectedGraph(vertices=vertices, edges=edges)
        generator = ConflictScheduleGenerator()
        schedule = generator.generate_schedule_from_acyclic_precedence_graph(
            graph
        )
        self.assert_precedence_graph_matches(schedule, graph)

        generator = ConflictScheduleGenerator(
            must_read_written = True
        )
        schedule = generator.generate_schedule_from_acyclic_precedence_graph(
            graph,
            
        )
        self.assert_precedence_graph_matches(schedule, graph)

        generator = ConflictScheduleGenerator(
            must_write_read = True
        )
        schedule = generator.generate_schedule_from_acyclic_precedence_graph(
            graph,
        )
        self.assert_precedence_graph_matches(schedule, graph)

        generator = ConflictScheduleGenerator(
            must_read_written = True,
            must_write_read = True
        )
        schedule = generator.generate_schedule_from_acyclic_precedence_graph(
            graph,
        )
        self.assert_precedence_graph_matches(schedule, graph)

    def test_cyclic_graph(self):
        """Test cyclic precedence: T1 -> T2, T2 -> T4, T4 -> T1, T1 -> T3"""
        vertices = [
            Vertex(id=1, label=1),
            Vertex(id=2, label=2),
            Vertex(id=3, label=3),
            Vertex(id=4, label=4)
        ]
        edges = [
            Edge(source=1, target=2, label=None),
            Edge(source=2, target=4, label=None),
            Edge(source=4, target=1, label=None),
            Edge(source=1, target=3, label=None)
        ]
        
        graph = DirectedGraph(vertices=vertices, edges=edges)
        
        generator = ConflictScheduleGenerator()
        schedule = generator.generate_schedule_from_cyclic_precedence_graph(
            graph
        )
        self.assert_precedence_graph_matches(schedule, graph)

        generator = ConflictScheduleGenerator(
            must_read_written = True,
        )
        schedule = generator.generate_schedule_from_cyclic_precedence_graph(
            graph,
        )
        self.assert_precedence_graph_matches(schedule, graph)

        generator = ConflictScheduleGenerator(
            must_write_read = True
        )
        schedule = generator.generate_schedule_from_cyclic_precedence_graph(
            graph,
        )
        self.assert_precedence_graph_matches(schedule, graph)

        generator = ConflictScheduleGenerator(
            must_read_written = True,
            must_write_read = True
        )
        schedule = generator.generate_schedule_from_cyclic_precedence_graph(
            graph,
        )
        self.assert_precedence_graph_matches(schedule, graph)

    def test_generate_conflict_equivalent_permutations(self):
        generator = ConflictScheduleGenerator()

        schedule = Schedule.parse("S_1 : W_1(A), W_1(B), R_2(A), W_2(C), R_3(B), W_3(D), R_4(C), R_4(D)")
        permutations = generator.generate_conflict_equivalent_permutations(schedule)
        self.assertEqual(len(permutations), 20)

        for i in range(5):
            self.assertTrue(schedule.is_conflict_equivalent_with(permutations[i]))
            self.assert_transaction_order_preserved(schedule, permutations[i])
        
        schedule = Schedule.parse("S_1 : R_1(A), W_1(A), R_1(B), W_1(B), R_2(A), W_2(A), R_2(C), W_2(C), R_3(B), W_3(B), R_3(D), W_3(D), R_4(C), R_4(D), W_4(C), W_4(D)")
        permutations = generator.generate_conflict_equivalent_permutations(schedule, max_permutations=100)
        self.assertEqual(len(permutations), 100)

        for i in range(5):
            self.assertTrue(schedule.is_conflict_equivalent_with(permutations[i]))
            self.assert_transaction_order_preserved(schedule, permutations[i])

    def test_generate_conflict_equivalent_permutations_preserves_transaction_order(self):
        generator = ConflictScheduleGenerator()
        schedule = Schedule.parse("S_1 : R_1(A), R_1(B), W_2(C), R_3(C)")

        permutations = generator.generate_conflict_equivalent_permutations(schedule)
        self.assertGreater(len(permutations), 0)

        for permutation in permutations:
            self.assert_transaction_order_preserved(schedule, permutation)

    def test_generate_random_conflict_equivalent_permutations(self):
        generator = ConflictScheduleGenerator()
        
        schedule = Schedule.parse("S_1 : W_1(A), W_1(B), R_2(A), W_2(C), R_3(B), W_3(D), R_4(C), R_4(D)")
        permutations = generator.generate_random_conflict_equivalent_permutations(
            schedule,
            count = 10
        )
        self.assertEqual(len(permutations), 10)

        for i in range(10):
            self.assertTrue(schedule.is_conflict_equivalent_with(permutations[i]))
            self.assert_transaction_order_preserved(schedule, permutations[i])
        
        schedule = Schedule.parse("S_1 : R_1(A), W_1(A), R_1(B), W_1(B), R_2(A), W_2(A), R_2(C), W_2(C), R_3(B), W_3(B), R_3(D), W_3(D), R_4(C), R_4(D), W_4(C), W_4(D)")
        permutations = generator.generate_random_conflict_equivalent_permutations(
            schedule,
            count = 20
        )
        self.assertEqual(len(permutations), 20)

        for i in range(20):
            self.assertTrue(schedule.is_conflict_equivalent_with(permutations[i]))
            self.assert_transaction_order_preserved(schedule, permutations[i])

    def test_generate_random_conflict_equivalent_permutations_preserves_transaction_order(self):
        generator = ConflictScheduleGenerator()
        schedule = Schedule.parse("S_1 : R_1(A), R_1(B), W_2(C), R_3(C)")

        permutations = generator.generate_random_conflict_equivalent_permutations(
            schedule,
            count=10
        )
        self.assertGreater(len(permutations), 0)

        for permutation in permutations:
            self.assert_transaction_order_preserved(schedule, permutation)

if __name__ == "__main__":
    unittest.main()
