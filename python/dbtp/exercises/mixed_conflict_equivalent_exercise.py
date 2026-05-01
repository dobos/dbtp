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

    @staticmethod
    def _schedule_key(schedule: Schedule) -> tuple[str, ...]:
        return tuple(str(op) for op in schedule.operations)

    @staticmethod
    def _swap_adjacent_conflicting_cross_tx_operations(
        reference: Schedule,
        operations: list[Operation],
        swaps: int
    ) -> list[Operation] | None:
        """
        Apply adjacent swaps between conflicting operations that belong to different
        transactions. Adjacent swaps preserve per-transaction operation order.
        """

        candidate = [Operation(tx=o.tx, op=o.op, item=o.item) for o in operations]

        for _ in range(swaps):
            swappable_indices = [
                i
                for i in range(len(candidate) - 1)
                if candidate[i].tx != candidate[i + 1].tx
                and candidate[i].is_in_conflict_with(candidate[i + 1])
            ]

            if not swappable_indices:
                return None

            idx = random.choice(swappable_indices)
            candidate[idx], candidate[idx + 1] = candidate[idx + 1], candidate[idx]

        return candidate

    def _generate_non_equivalent_schedules(
        self,
        reference: Schedule,
        count: int,
        max_attempts: int = None
    ) -> list[Schedule]:
        
        """
        Generate schedules that are not conflict-equivalent to the reference schedule.

        Candidates are produced by swapping adjacent operations only when they are
        conflicting operations from different transactions. This guarantees that the
        relative operation order inside each transaction is preserved.
        """

        if count <= 0:
            return []

        if max_attempts is None:
            max_attempts = count * 300

        non_equivalent = []
        seen = set()
        base_ops = reference.operations
        attempts = 0
        reference_key = self._schedule_key(reference)

        while len(non_equivalent) < count and attempts < max_attempts:
            attempts += 1

            max_swaps = max(1, min(4, len(base_ops) - 1))
            swap_count = random.randint(1, max_swaps)
            swapped = self._swap_adjacent_conflicting_cross_tx_operations(
                reference,
                base_ops,
                swaps=swap_count
            )
            if swapped is None:
                continue

            candidate = self._copy_schedule_with_operations(reference, swapped)
            candidate_key = self._schedule_key(candidate)

            if candidate_key == reference_key:
                continue

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
        reference_key = self._schedule_key(reference)

        equivalent = self._generate_conflict_equivalent_schedules(
            reference,
            self.num_equivalent
        )

        non_equivalent = self._generate_non_equivalent_schedules(
            reference,
            self.num_non_equivalent
        )

        # Keep equivalent schedules different from the reference schedule.
        adjusted_equivalent = []
        for schedule in equivalent:
            candidate = schedule
            for _ in range(20):
                candidate = self._random_conflict_equivalent_permutation(candidate)
                if self._schedule_key(candidate) != reference_key:
                    break

            if self._schedule_key(candidate) == reference_key:
                raise RuntimeError(
                    "Could not generate a conflict-equivalent schedule different from the reference"
                )

            adjusted_equivalent.append(candidate)
        equivalent = adjusted_equivalent

        # Non-equivalent schedules are already different by construction; keep this
        # defensive check so we never output the same schedule as the reference.
        adjusted_non_equivalent = []
        for schedule in non_equivalent:
            candidate = self._random_conflict_equivalent_permutation(schedule)
            if self._schedule_key(candidate) == reference_key:
                candidate = schedule
            if self._schedule_key(candidate) == reference_key:
                raise RuntimeError(
                    "Could not keep non-equivalent schedule different from the reference"
                )
            adjusted_non_equivalent.append(candidate)
        non_equivalent = adjusted_non_equivalent

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
