import unittest

from dbtp.exercises.mixed_conflict_equivalent_exercise import MixedConflictEquivalentExercise
from dbtp.schedule import Schedule


class MixedConflictEquivalentExerciseTest(unittest.TestCase):
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
            self.assert_transaction_order_preserved(reference, candidate)

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

    def test_generate_non_equivalent_never_matches_reference_schedule(self):
        exercise = MixedConflictEquivalentExercise()
        exercise.seed = 11
        exercise._setup_random()
        reference = Schedule.parse("S_1 : W_1(A), R_2(A), W_2(B), R_1(B)")

        generated = exercise._generate_non_equivalent_schedules(
            reference,
            count=2,
            max_attempts=5000
        )

        reference_key = tuple(str(op) for op in reference.operations)
        for candidate in generated:
            candidate_key = tuple(str(op) for op in candidate.operations)
            self.assertNotEqual(candidate_key, reference_key)


if __name__ == "__main__":
    unittest.main()
