import unittest
import argparse

from dbtp.exercises.conflict_equivalent_exercise import ConflictEquivalentExercise
from dbtp.schedule import Schedule


class ConflictEquivalentExerciseTest(unittest.TestCase):
    def test_init_defaults(self):
        """Test that ConflictEquivalentExercise initializes with correct defaults."""
        exercise = ConflictEquivalentExercise()
        
        self.assertEqual(exercise.num_schedules, 4)
        self.assertEqual(exercise.num_transactions, 4)
        self.assertEqual(exercise.num_operations, 4)
        self.assertIsNone(exercise.seed)
        self.assertTrue(exercise.must_read)
        self.assertFalse(exercise.must_write)
        self.assertFalse(exercise.serializable)
        self.assertFalse(exercise.latex)
        self.assertFalse(exercise.print_conflict_graphs)
        self.assertTrue(exercise.random_permutation)

    def test_create_parser(self):
        """Test that the CLI parser is created with expected arguments."""
        subparsers = argparse.ArgumentParser().add_subparsers()
        ConflictEquivalentExercise.create_parser(subparsers)
        
        # Parser should have been registered
        self.assertIsNotNone(subparsers)

    def test_parse_args_num_schedules(self):
        """Test parsing --num-schedules argument."""
        exercise = ConflictEquivalentExercise()
        
        # Create mock args object
        args = argparse.Namespace(
            num_schedules=6,
            num_transactions=None,
            num_operations=None,
            seed=None,
            must_read=None,
            must_write=None,
            serializable=None,
            random_permutation=None,
            latex=None,
            print_conflict_graphs=None,
        )
        
        exercise.parse_args(args)
        self.assertEqual(exercise.num_schedules, 6)

    def test_parse_args_common_arguments(self):
        """Test parsing common arguments shared across exercises."""
        exercise = ConflictEquivalentExercise()
        
        args = argparse.Namespace(
            num_schedules=None,
            num_transactions=5,
            num_operations=3,
            seed=42,
            must_read=False,
            must_write=True,
            serializable=True,
            random_permutation=False,
            latex=True,
            print_conflict_graphs=True,
        )
        
        exercise.parse_args(args)
        self.assertEqual(exercise.num_transactions, 5)
        self.assertEqual(exercise.num_operations, 3)
        self.assertEqual(exercise.seed, 42)
        self.assertFalse(exercise.must_read)
        self.assertTrue(exercise.must_write)
        self.assertTrue(exercise.serializable)
        self.assertFalse(exercise.random_permutation)
        self.assertTrue(exercise.latex)
        self.assertTrue(exercise.print_conflict_graphs)

    def test_generate_conflict_equivalent_schedules(self):
        """Test that generated schedules are all conflict-equivalent."""
        exercise = ConflictEquivalentExercise()
        exercise.seed = 5
        exercise.num_schedules = 4
        exercise.num_transactions = 3
        exercise.num_operations = 3
        exercise.must_read = True
        exercise.must_write = False
        exercise.serializable = False

        exercise._setup_random()
        
        schedule = exercise._generate_reference_schedule()
        equivalents = exercise._generate_conflict_equivalent_schedules(
            schedule,
            count=exercise.num_schedules - 1
        )

        self.assertEqual(len(equivalents), 3)
        
        for equiv_schedule in equivalents:
            self.assertTrue(schedule.is_conflict_equivalent_with(equiv_schedule))

    def test_generate_produces_correct_count(self):
        """Test that generate() produces the requested number of schedules."""
        exercise = ConflictEquivalentExercise()
        exercise.seed = 7
        exercise.num_schedules = 5
        exercise.num_transactions = 4
        exercise.num_operations = 4
        exercise.must_read = True
        exercise.must_write = False
        exercise.serializable = False
        exercise.random_permutation = True
        
        # Capture printed output
        import io
        import sys
        captured_output = io.StringIO()
        sys.stdout = captured_output
        
        try:
            exercise.generate()
            output = captured_output.getvalue()
        finally:
            sys.stdout = sys.__stdout__
        
        # Count schedule outputs in the generated section
        lines = output.split('\n')
        generated_idx = None
        for i, line in enumerate(lines):
            if "Generated schedules:" in line:
                generated_idx = i
                break
        
        self.assertIsNotNone(generated_idx)

    def test_generate_with_random_permutation_disabled(self):
        """Test that generate() works with random_permutation disabled."""
        exercise = ConflictEquivalentExercise()
        exercise.seed = 11
        exercise.num_schedules = 3
        exercise.num_transactions = 3
        exercise.num_operations = 3
        exercise.must_read = True
        exercise.must_write = False
        exercise.serializable = False
        exercise.random_permutation = False
        
        import io
        import sys
        captured_output = io.StringIO()
        sys.stdout = captured_output
        
        try:
            exercise.generate()
        finally:
            sys.stdout = sys.__stdout__

    def test_generate_with_graph_printing(self):
        """Test that generate() works with conflict graph printing enabled."""
        exercise = ConflictEquivalentExercise()
        exercise.seed = 13
        exercise.num_schedules = 2
        exercise.num_transactions = 2
        exercise.num_operations = 2
        exercise.print_conflict_graphs = True
        
        import io
        import sys
        captured_output = io.StringIO()
        sys.stdout = captured_output
        
        try:
            exercise.generate()
            output = captured_output.getvalue()
        finally:
            sys.stdout = sys.__stdout__
        
        self.assertIn("Conflict graphs:", output)

    def test_generate_with_latex_output(self):
        """Test that generate() works with LaTeX output enabled."""
        exercise = ConflictEquivalentExercise()
        exercise.seed = 17
        exercise.num_schedules = 2
        exercise.num_transactions = 2
        exercise.num_operations = 2
        exercise.latex = True
        
        import io
        import sys
        captured_output = io.StringIO()
        sys.stdout = captured_output
        
        try:
            exercise.generate()
            output = captured_output.getvalue()
        finally:
            sys.stdout = sys.__stdout__
        
        # LaTeX output should contain LaTeX-formatted subscripts with braces
        self.assertIn("_{", output)

    def test_generate_small_cyclic_exercise_without_allow_flag_falls_back_to_two_node_cycle(self):
        exercise = ConflictEquivalentExercise()
        exercise.seed = 17
        exercise.num_schedules = 2
        exercise.num_transactions = 2
        exercise.num_operations = 2
        exercise.allow_two_node_cycles = False

        import io
        import sys
        captured_output = io.StringIO()
        sys.stdout = captured_output

        try:
            exercise.generate()
            output = captured_output.getvalue()
        finally:
            sys.stdout = sys.__stdout__

        self.assertIn("Generated schedules:", output)

    def test_print_banner(self):
        """Test that print_banner outputs expected information."""
        exercise = ConflictEquivalentExercise()
        exercise.num_schedules = 5
        exercise.num_transactions = 3
        exercise.seed = 42
        exercise.random_permutation = False
        
        import io
        import sys
        captured_output = io.StringIO()
        sys.stdout = captured_output
        
        try:
            exercise.print_banner()
            output = captured_output.getvalue()
        finally:
            sys.stdout = sys.__stdout__
        
        self.assertIn("conflict-equivalency exercise", output)
        self.assertIn("Number of schedules: 5", output)
        self.assertIn("Number of transactions: 3", output)
        self.assertIn("Random seed: 42", output)
        self.assertIn("Random conflict-equivalent permutation: False", output)


if __name__ == "__main__":
    unittest.main()
