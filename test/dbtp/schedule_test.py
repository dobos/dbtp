import unittest

from dbtp import Schedule, Operation, OperationType

class TestSchedule(unittest.TestCase):
    def test_str(self):
        t = Schedule(
            id = 1,
            operations=[
                Operation(tx=1, op=OperationType.READ, item="A"),
                Operation(tx=1, op=OperationType.WRITE, item="B")
            ]
        )
        
        res = str(t)
        self.assertEqual(res, "S_1 : R_1(A), W_1(B)")

    def test_latex(self):
        t = Schedule(
            id = 1,
            operations=[
                Operation(tx=1, op=OperationType.READ, item="A"),
                Operation(tx=1, op=OperationType.WRITE, item="B")
            ]
        )
        
        res = t.latex()
        self.assertEqual(res, "S_{1} : r_{1}(A), w_{1}(B)")

    def test_parse(self):
        s = "S_2 : R_2(X), W_2(Y), COMMIT_2"
        schedule = Schedule.parse(s)
        
        self.assertEqual(schedule.id, 2)
        self.assertEqual(len(schedule.operations), 3)
        
        self.assertEqual(schedule.operations[0].tx, 2)
        self.assertEqual(schedule.operations[0].op, OperationType.READ)
        self.assertEqual(schedule.operations[0].item, "X")
        
        self.assertEqual(schedule.operations[1].tx, 2)
        self.assertEqual(schedule.operations[1].op, OperationType.WRITE)
        self.assertEqual(schedule.operations[1].item, "Y")
        
        self.assertEqual(schedule.operations[2].tx, 2)
        self.assertEqual(schedule.operations[2].op, OperationType.COMMIT)
        self.assertIsNone(schedule.operations[2].item)

    def test_build_conflict_graph(self):
        schedule = Schedule.parse("S_1 : R_1(A), W_2(A), W_1(B), R_2(B)")        
        graph = schedule.build_conflict_graph()
        
        # There should be edges representing conflicts
        expected_edges = {
            (0, 1),  # R_1(A) -> W_2(A)
            (2, 3),  # W_1(B) -> R_2(B)
        }
        
        actual_edges = set((edge.source, edge.target) for edge in graph.edges.values())
        
        self.assertEqual(actual_edges, expected_edges)

    def test_is_conflict_equivalent_with(self):
        schedule1 = Schedule.parse("S_1 : R_1(A), W_2(A)")
        schedule2 = Schedule.parse("S_2 : W_2(A), R_1(A)")
        self.assertFalse(schedule1.is_conflict_equivalent_with(schedule2))
        
        schedule3 = Schedule.parse("S_3 : R_1(A), W_2(A)")
        self.assertTrue(schedule1.is_conflict_equivalent_with(schedule3))

    def test_build_precedence_graph(self):
        schedule = Schedule.parse("S_1 : R_1(A), W_2(A), W_1(B), R_2(B)")
        graph = schedule.build_precedence_graph()
        
        expected_edges = { (1, 2) }
        actual_edges = set((edge.source, edge.target) for edge in graph.edges.values())

        self.assertEqual(actual_edges, expected_edges)

        schedule = Schedule.parse("S_1 : R_1(A), W_2(B), W_1(B), R_2(B)")
        graph = schedule.build_precedence_graph()

        expected_edges = { (1, 2), (2, 1) }
        actual_edges = set((edge.source, edge.target) for edge in graph.edges.values())

        self.assertEqual(actual_edges, expected_edges)

    def test_is_conflict_serializable(self):
        # Non-serializable: cyclic dependency T1 -> T2 -> T1
        schedule1 = Schedule.parse("S_1 : R_1(A), W_2(A), R_2(B), W_1(B)")
        self.assertFalse(schedule1.is_conflict_serializable())
        
        # Serializable: acyclic dependency T1 -> T2
        schedule2 = Schedule.parse("S_2 : R_1(A), W_1(A), R_2(A), W_2(B)")
        self.assertTrue(schedule2.is_conflict_serializable())

    def test_serialize(self):
        schedule = Schedule.parse("S_1 : R_1(A), W_2(A), W_1(B), R_2(B)")
        serial = schedule.serialize()
        
        expected = "S_1 : R_1(A), W_1(B), W_2(A), R_2(B)"
        self.assertEqual(str(serial), expected)        
