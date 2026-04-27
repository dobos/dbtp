import unittest

from dbtp import Schedule, Operation, OperationType
from dbtp.directedgraph import DirectedGraph, CyclicGraphError

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

    def test_parse_locks(self):
        s = "S_3 : SL_1(A), XL_2(B), U_1(A), L_1(C)"
        schedule = Schedule.parse(s)
        
        self.assertEqual(schedule.id, 3)
        self.assertEqual(len(schedule.operations), 4)
        
        self.assertEqual(schedule.operations[0].tx, 1)
        self.assertEqual(schedule.operations[0].op, OperationType.SLOCK)
        self.assertEqual(schedule.operations[0].item, "A")
        
        self.assertEqual(schedule.operations[1].tx, 2)
        self.assertEqual(schedule.operations[1].op, OperationType.XLOCK)
        self.assertEqual(schedule.operations[1].item, "B")
        
        self.assertEqual(schedule.operations[2].tx, 1)
        self.assertEqual(schedule.operations[2].op, OperationType.UNLOCK)
        self.assertEqual(schedule.operations[2].item, "A")
        
        self.assertEqual(schedule.operations[3].tx, 1)
        self.assertEqual(schedule.operations[3].op, OperationType.LOCK)
        self.assertEqual(schedule.operations[3].item, "C")

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

    def test_build_wait_for_graph(self):
        schedule = Schedule.parse(
            "S_1 : SL_1(A), XL_2(A), U_1(A), XL_1(B), U_2(A), U_1(B)"
        )
        graph = schedule.build_wait_for_graph()
        
        expected_edges = { (2, 1) }  # T1 waits for T2 due to XLOCK on A
        actual_edges = set((edge.source, edge.target) for edge in graph.edges.values())
        
        self.assertEqual(actual_edges, expected_edges)

        schedule = Schedule.parse(
            "S_1 : SL_1(A), SL_2(A), XL_3(A), XL_1(B), U_2(A), U_1(B)"
        )
        graph = schedule.build_wait_for_graph()

        expected_edges = { (3, 1), (3, 2) }  # T3 waits for T1 and T2 due to XLOCK on A
        actual_edges = set((edge.source, edge.target) for edge in graph.edges.values())

        self.assertEqual(actual_edges, expected_edges)

    def test_build_wait_for_graph_deadlock(self):
        schedule = Schedule.parse(
            "S_1 : XL_1(A), XL_2(B), XL_1(B), XL_2(A)"
        )
        graph = schedule.build_wait_for_graph()
        expected_edges = { (1, 2), (2, 1) }  # Deadlock: T1 waits for T2 and T2 waits for T1
        actual_edges = set((edge.source, edge.target) for edge in graph.edges.values())
                           
        self.assertEqual(actual_edges, expected_edges)
        with self.assertRaises(CyclicGraphError):
            graph.topological_sort()

    def test_has_deadlock(self):
        # Deadlock scenario
        schedule1 = Schedule.parse(
            "S_1 : XL_1(A), XL_2(B), XL_1(B), XL_2(A)"
        )
        self.assertTrue(schedule1.has_deadlock())
        
        # No deadlock scenario
        schedule2 = Schedule.parse(
            "S_2 : SL_1(A), U_1(A), XL_2(A)"
        )
        self.assertFalse(schedule2.has_deadlock())

    def test_is_legal(self):
        # Legal schedule with proper locking
        schedule1 = Schedule.parse(
            "S_1 : SL_1(A), R_1(A), U_1(A), XL_2(B), W_2(B), U_2(B)"
        )
        self.assertTrue(schedule1.is_legal())
        
        # Illegal schedule: operation without lock
        schedule2 = Schedule.parse(
            "S_2 : R_1(A), W_1(A)"
        )
        self.assertFalse(schedule2.is_legal())
        
        # Illegal schedule: unlock without lock
        schedule3 = Schedule.parse(
            "S_3 : U_1(A), SL_1(A), R_1(A), U_1(A)"
        )
        self.assertTrue(schedule3.is_legal())

    def test_is_two_phase_locked(self):
        # 2PL compliant schedule
        schedule1 = Schedule.parse(
            "S_1 : SL_1(A), R_1(A), XL_1(B), W_1(B), U_1(A), U_1(B)"
        )
        self.assertTrue(schedule1.is_two_phase_locked())
        
        # Non-2PL schedule: lock after unlock
        schedule2 = Schedule.parse(
            "S_2 : SL_1(A), R_1(A), U_1(A), XL_1(B), W_1(B), U_1(B)"
        )
        self.assertFalse(schedule2.is_two_phase_locked())

    def test_add_locks_to_schedule(self):
        schedule = Schedule.parse("S_1 : R_1(A), W_1(B), W_1(A)")

        expected = "S_1 : R_1(A), W_1(B), W_1(A)"
        self.assertEqual(str(schedule), expected)
        
        locked = schedule.add_locks()
        expected = "S_1 : L_1(A), R_1(A), L_1(B), W_1(B), U_1(B), W_1(A), U_1(A)"
        self.assertEqual(str(locked), expected)
        
        locked = schedule.add_locks(use_shared_locks=True)
        expected = "S_1 : SL_1(A), R_1(A), XL_1(B), W_1(B), U_1(B), XL_1(A), W_1(A), U_1(A)"
        self.assertEqual(str(locked), expected)

    def test_add_locks_to_schedule_two_phase(self):
        schedule = Schedule.parse("S_1 : R_1(A), W_1(B), W_1(A)")

        locked = schedule.add_locks_two_phase()
        expected = "S_1 : L_1(A), R_1(A), L_1(B), W_1(B), U_1(B), W_1(A), U_1(A)"
        self.assertEqual(str(locked), expected)

        locked = schedule.add_locks_two_phase(use_shared_locks=True)
        expected = "S_1 : SL_1(A), R_1(A), XL_1(B), W_1(B), XL_1(A), U_1(B), W_1(A), U_1(A)"
        self.assertEqual(str(locked), expected)

        schedule = Schedule.parse("S_1 : R_1(A), W_1(B), R_2(B), W_1(A), W_2(B)")
        locked = schedule.add_locks_two_phase(use_shared_locks=True)
        expected = "S_1 : SL_1(A), R_1(A), XL_1(B), W_1(B), SL_2(B), R_2(B), XL_1(A), U_1(B), W_1(A), U_1(A), XL_2(B), W_2(B), U_2(B)"
        self.assertEqual(str(locked), expected)