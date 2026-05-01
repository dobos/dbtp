import random

from ..operation import Operation
from ..schedule import Schedule
from .conflict_exercise import ConflictExercise


class MixedConflictEquivalentExercise(ConflictExercise):
    def __init__(self):
        super().__init__()
        self.num_equivalent = 2
        self.num_non_equivalent = 2

    @staticmethod
    def create_parser(subparsers):
        parser = subparsers.add_parser(
            "conf-eq-mix",
            help="Generate mixed conflict-equivalency exercise"
        )
        parser.add_argument(
            "--num-equivalent",
            type=int,
            help="Number of conflict-equivalent schedules to generate"
        )
        parser.add_argument(
            "--num-non-equivalent",
            type=int,
            help="Number of non-conflict-equivalent schedules to generate"
        )
        MixedConflictEquivalentExercise._add_common_arguments(parser)

    def parse_args(self, args):
        super().parse_args(args)
        if args.num_equivalent is not None:
            self.num_equivalent = args.num_equivalent
        if args.num_non_equivalent is not None:
            self.num_non_equivalent = args.num_non_equivalent

    def print_banner(self):
        print("Generating mixed conflict-equivalency exercise with the following parameters:")
        print(f"Number of equivalent schedules: {self.num_equivalent}")
        print(f"Number of non-equivalent schedules: {self.num_non_equivalent}")
        super().print_banner()

    @staticmethod
    def _copy_schedule_with_operations(reference: Schedule, operations) -> Schedule:
        return Schedule(
            id=reference.id,
            operations=[Operation(tx=o.tx, op=o.op, item=o.item) for o in operations]
        )

    def _generate_non_equivalent_schedules(
        self,
        reference: Schedule,
        count: int,
        max_attempts: int = None
    ) -> list[Schedule]:
        
        """
        Generate schedules that are not conflict-equivalent to the reference schedule by shuffling
        the operations and checking for equivalence. This is a brute-force approach
        and may not be efficient for large schedules or high counts, but it serves the
        purpose of generating non-equivalent schedules for exercises.
        """

        if count <= 0:
            return []

        if max_attempts is None:
            max_attempts = count * 300

        non_equivalent = []
        seen = set()
        base_ops = reference.operations
        attempts = 0

        while len(non_equivalent) < count and attempts < max_attempts:
            attempts += 1

            shuffled = random.sample(base_ops, len(base_ops))
            candidate = self._copy_schedule_with_operations(reference, shuffled)
            candidate = self._random_conflict_equivalent_permutation(candidate)
            candidate_key = tuple(str(op) for op in candidate.operations)

            if candidate_key in seen:
                continue

            seen.add(candidate_key)

            if not reference.is_conflict_equivalent_with(candidate):
                non_equivalent.append(candidate)

        if len(non_equivalent) < count:
            raise RuntimeError(
                "Could not generate enough non-conflict-equivalent schedules with the current settings"
            )

        return non_equivalent

    def generate(self):
        print("Generating schedules...")

        self._setup_random()

        reference = self._random_conflict_equivalent_permutation(
            self._generate_reference_schedule()
        )
        reference.id = 1

        equivalent = self._generate_conflict_equivalent_schedules(
            reference,
            self.num_equivalent
        )

        non_equivalent = self._generate_non_equivalent_schedules(
            reference,
            self.num_non_equivalent
        )

        equivalent = [self._random_conflict_equivalent_permutation(s) for s in equivalent]
        non_equivalent = [self._random_conflict_equivalent_permutation(s) for s in non_equivalent]

        for schedule in equivalent:
            if not reference.is_conflict_equivalent_with(schedule):
                raise RuntimeError("Verification failed: expected conflict-equivalent schedule")

        for schedule in non_equivalent:
            if reference.is_conflict_equivalent_with(schedule):
                raise RuntimeError("Verification failed: non-equivalent schedule is conflict-equivalent")

        # Permute each class before composing the final print order.
        random.shuffle(equivalent)
        random.shuffle(non_equivalent)

        generated = equivalent + non_equivalent
        random.shuffle(generated)

        for i, schedule in enumerate(generated, start=2):
            schedule.id = i

        equivalent_ids = [schedule.id for schedule in equivalent]
        non_equivalent_ids = [schedule.id for schedule in non_equivalent]

        print("Reference schedule:")
        self._print_schedule(reference)

        self._print_schedules(generated)

        if self.print_conflict_graphs:
            print("Conflict graphs:")
            self._print_conflict_graph(reference)
            self._print_conflict_graphs(generated, heading=None)

        print("Solutions:")
        print(f"Equivalent to reference: {sorted(equivalent_ids)}")
        print(f"Not equivalent to reference: {sorted(non_equivalent_ids)}")
