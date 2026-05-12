import argparse
import io
import sys
import unittest

from dbtp.exercises.deadlock_exercise import DeadlockExercise
from dbtp.schedule import Schedule


class DeadlockExerciseTest(unittest.TestCase):
    def test_parse_args(self):
        exercise = DeadlockExercise()

        args = argparse.Namespace(
            num_transactions=5,
            num_operations=6,
            seed=7,
            deadlocking=False,
            allow_two_node_cycles=True,
            latex=True,
            print_wait_for_graph=True,
            graph_after_ops=8,
        )

        exercise.parse_args(args)

        self.assertEqual(exercise.num_transactions, 5)
        self.assertEqual(exercise.num_operations, 6)
        self.assertEqual(exercise.seed, 7)
        self.assertFalse(exercise.deadlocking)
        self.assertTrue(exercise.allow_two_node_cycles)
        self.assertTrue(exercise.latex)
        self.assertTrue(exercise.print_wait_for_graph)
        self.assertEqual(exercise.graph_after_ops, 8)

    def test_generate_deadlocking_schedule_requires_three_nodes_without_two_node_cycles(self):
        exercise = DeadlockExercise()
        exercise.seed = 13
        exercise.num_transactions = 2
        exercise.num_operations = 2
        exercise.deadlocking = True
        exercise.allow_two_node_cycles = False

        with self.assertRaises(ValueError):
            exercise._generate_schedule()

    def test_generate_deadlocking_schedule_allows_two_node_cycles(self):
        exercise = DeadlockExercise()
        exercise.seed = 13
        exercise.num_transactions = 2
        exercise.num_operations = 2
        exercise.deadlocking = True
        exercise.allow_two_node_cycles = True

        schedule = exercise._generate_schedule()

        self.assertTrue(schedule.is_two_phase_locked())
        self.assertTrue(schedule.is_legal())
        self.assertTrue(schedule.has_deadlock())

    def test_generate_deadlocking_schedule(self):
        exercise = DeadlockExercise()
        exercise.seed = 13
        exercise.num_transactions = 4
        exercise.num_operations = 4
        exercise.deadlocking = True

        schedule = exercise._generate_schedule()

        self.assertTrue(schedule.is_two_phase_locked())
        self.assertTrue(schedule.is_legal())
        self.assertTrue(schedule.has_deadlock())

    def test_generate_non_deadlocking_schedule(self):
        exercise = DeadlockExercise()
        exercise.seed = 19
        exercise.num_transactions = 4
        exercise.num_operations = 4
        exercise.deadlocking = False

        schedule = exercise._generate_schedule()

        self.assertTrue(schedule.is_two_phase_locked())
        self.assertTrue(schedule.is_legal())
        self.assertFalse(schedule.has_deadlock())

    def test_wait_for_graph_after_ops(self):
        schedule = Schedule.parse(
            "S_1 : XL_1(A), XL_2(B), XL_1(B), XL_2(A), U_1(A), U_2(B)"
        )

        graph = DeadlockExercise.wait_for_graph_after_ops(schedule, 4)
        actual_edges = set((edge.source, edge.target) for edge in graph.edges.values())

        self.assertEqual(actual_edges, {(1, 2), (2, 1)})

    def test_print_wait_for_graph_latex(self):
        exercise = DeadlockExercise()
        exercise.seed = 23
        exercise.num_transactions = 4
        exercise.num_operations = 4
        exercise.deadlocking = True
        exercise.print_wait_for_graph = True
        exercise.graph_after_ops = 6
        exercise.latex = True

        captured = io.StringIO()
        sys_stdout = sys.stdout
        sys.stdout = captured
        try:
            exercise.generate()
        finally:
            sys.stdout = sys_stdout

        output = captured.getvalue()
        self.assertIn("Wait-for graph", output)
        self.assertIn("\\begin{tikzpicture}", output)


if __name__ == "__main__":
    unittest.main()
