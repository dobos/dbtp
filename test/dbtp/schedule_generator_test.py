import unittest

from dbtp import (
    DirectedGraph,
    Vertex,
    Edge,
    OperationType,
    ScheduleGenerator,
    Schedule
)

class ScheduleGeneratorTest(unittest.TestCase):
    
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
        schedule = ScheduleGenerator.from_precedence_graph(graph)
        
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
        schedule = ScheduleGenerator.from_precedence_graph(graph)
        
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

        schedule = ScheduleGenerator.from_precedence_graph(
            graph
        )
        self.assertEqual(str(schedule), "S_1 : W_1(A), W_1(B), R_2(A), W_2(C), R_3(B), W_3(D), R_4(C), R_4(D)")

        schedule = ScheduleGenerator.from_precedence_graph(
            graph,
            must_read_written = True
        )
        self.assertEqual(str(schedule), "S_1 : R_1(A), W_1(A), R_1(B), W_1(B), R_2(A), R_2(C), W_2(C), R_3(B), R_3(D), W_3(D), R_4(C), R_4(D)")

        schedule = ScheduleGenerator.from_precedence_graph(
            graph,
            must_write_read = True
        )
        self.assertEqual(str(schedule), "S_1 : W_1(A), W_1(B), R_2(A), W_2(A), W_2(C), R_3(B), W_3(B), W_3(D), R_4(C), R_4(D), W_4(C), W_4(D)")

        schedule = ScheduleGenerator.from_precedence_graph(
            graph,
            must_read_written = True,
            must_write_read = True
        )
        self.assertEqual(str(schedule), "S_1 : R_1(A), W_1(A), R_1(B), W_1(B), R_2(A), W_2(A), R_2(C), W_2(C), R_3(B), W_3(B), R_3(D), W_3(D), R_4(C), R_4(D), W_4(C), W_4(D)")

    def test_generate_conflict_equivalent_permutations(self):
        schedule = Schedule.parse("S_1 : W_1(A), W_1(B), R_2(A), W_2(C), R_3(B), W_3(D), R_4(C), R_4(D)")
        permutations = ScheduleGenerator.generate_conflict_equivalent_permutations(schedule)
        self.assertEqual(len(permutations), 2520)
        
        schedule = Schedule.parse("S_1 : R_1(A), W_1(A), R_1(B), W_1(B), R_2(A), W_2(A), R_2(C), W_2(C), R_3(B), W_3(B), R_3(D), W_3(D), R_4(C), R_4(D), W_4(C), W_4(D)")
        permutations = ScheduleGenerator.generate_conflict_equivalent_permutations(schedule, max_permutations=100)
        self.assertEqual(len(permutations), 100)

    def test_generate_random_conflict_equivalent_permutations(self):
        schedule = Schedule.parse("S_1 : W_1(A), W_1(B), R_2(A), W_2(C), R_3(B), W_3(D), R_4(C), R_4(D)")
        random_permutations = ScheduleGenerator.generate_random_conflict_equivalent_permutations(
            schedule,
            count = 10
        )
        self.assertEqual(len(random_permutations), 10)
        
        schedule = Schedule.parse("S_1 : R_1(A), W_1(A), R_1(B), W_1(B), R_2(A), W_2(A), R_2(C), W_2(C), R_3(B), W_3(B), R_3(D), W_3(D), R_4(C), R_4(D), W_4(C), W_4(D)")
        random_permutations = ScheduleGenerator.generate_random_conflict_equivalent_permutations(
            schedule,
            count = 20
        )
        self.assertEqual(len(random_permutations), 20)

if __name__ == "__main__":
    unittest.main()
