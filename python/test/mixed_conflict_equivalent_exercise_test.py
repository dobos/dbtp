import unittest

from dbtp.exercises.mixed_conflict_equivalent_exercise import MixedConflictEquivalentExercise
from dbtp.schedule import Schedule


class MixedConflictEquivalentExerciseTest(unittest.TestCase):
    def test_generate_non_equivalent_schedules_count_and_equivalence(self):
        exercise = MixedConflictEquivalentExercise()
        reference = Schedule.parse("S_1 : W_1(A), R_2(A), W_2(B), R_1(B)")

        generated = exercise._generate_non_equivalent_schedules(
            reference,
            count=3,
            max_attempts=5000
        )

        self.assertEqual(len(generated), 3)
        for candidate in generated:
            self.assertFalse(reference.is_conflict_equivalent_with(candidate))

    def test_generate_mixed_exercise_produces_requested_counts(self):
        exercise = MixedConflictEquivalentExercise()
        exercise.seed = 7
        exercise.num_equivalent = 3
        exercise.num_non_equivalent = 2
        exercise.num_transactions = 4
        exercise.num_operations = 4
        exercise.must_read = True
        exercise.must_write = False
        exercise.serializable = False

        # The method should complete without verification errors.
        exercise.generate()


if __name__ == "__main__":
    unittest.main()
