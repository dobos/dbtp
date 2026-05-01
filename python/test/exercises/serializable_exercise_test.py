import unittest

from dbtp.exercises.serializable_exercise import SerializableExercise
from dbtp.schedule import Schedule
from dbtp.conflict_schedule_generator import ConflictScheduleGenerator


class SerializableExerciseTest(unittest.TestCase):
    def test_random_conflict_equivalent_permutations_preserve_class(self):
        original = Schedule.parse("S_1 : W_1(A), R_2(A), W_2(B), R_1(B)")
        generator = ConflictScheduleGenerator(max_attempts=200)
        permutations = generator.generate_random_conflict_equivalent_permutations(
            original,
            count=5,
        )

        self.assertTrue(len(permutations) > 0)

        shuffled = permutations[0]

        self.assertTrue(original.is_conflict_equivalent_with(shuffled))
        self.assertEqual(
            original.is_conflict_serializable(),
            shuffled.is_conflict_serializable(),
        )

    def test_generate_schedules_by_serializability(self):
        exercise = SerializableExercise()
        exercise.seed = 7
        exercise.num_transactions = 4
        exercise.num_operations = 4

        serializable = exercise._generate_schedules_by_serializability(
            count=2,
            serializable=True,
            max_attempts=5000,
        )
        non_serializable = exercise._generate_schedules_by_serializability(
            count=2,
            serializable=False,
            max_attempts=5000,
        )

        self.assertEqual(len(serializable), 2)
        self.assertEqual(len(non_serializable), 2)

        for schedule in serializable:
            self.assertTrue(schedule.is_conflict_serializable())

        for schedule in non_serializable:
            self.assertFalse(schedule.is_conflict_serializable())

    def test_generate_serializable_exercise_produces_requested_counts(self):
        exercise = SerializableExercise()
        exercise.seed = 11
        exercise.num_serializable = 3
        exercise.num_non_serializable = 2
        exercise.num_transactions = 4
        exercise.num_operations = 4
        exercise.must_read = True
        exercise.must_write = False

        # The method should complete without verification errors.
        exercise.generate()


if __name__ == "__main__":
    unittest.main()