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
        self.assertEqual(res, "T_1 : R_1(A), W_1(B)")

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
        ops = [
            Operation(tx=1, op=OperationType.READ, item="A"),
            Operation(tx=2, op=OperationType.WRITE, item="A"),
            Operation(tx=1, op=OperationType.WRITE, item="B"),
            Operation(tx=2, op=OperationType.READ, item="B"),
        ]
        schedule = Schedule(id=1, operations=ops)
        
        graph = schedule.build_conflict_graph()
        
        # There should be edges representing conflicts
        expected_edges = {
            (0, 1),  # R_1(A) -> W_2(A)
            (2, 3),  # W_1(B) -> R_2(B)
        }
        
        actual_edges = set((edge.source, edge.target) for edge in graph.edges.values())
        
        self.assertEqual(actual_edges, expected_edges)