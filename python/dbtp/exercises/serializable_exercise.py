import random

from ..schedule import Schedule
from ..schedulegenerator import ScheduleGenerator
from .conflict_exercise import ConflictExercise


class SerializableExercise(ConflictExercise):
    def __init__(self):
        super().__init__()
        self.num_serializable = 2
        self.num_non_serializable = 2

    @staticmethod
    def create_parser(subparsers):
        parser = subparsers.add_parser(
            "serializable",
            help="Generate mixed serializable/non-serializable exercise"
        )
        parser.add_argument(
            "--num-serializable",
            type=int,
            help="Number of conflict-serializable schedules to generate"
        )
        parser.add_argument(
            "--num-non-serializable",
            type=int,
            help="Number of non-conflict-serializable schedules to generate"
        )
        SerializableExercise._add_common_arguments(parser, include_serializable=False)

    def parse_args(self, args):
        super().parse_args(args)
        if args.num_serializable is not None:
            self.num_serializable = args.num_serializable
        if args.num_non_serializable is not None:
            self.num_non_serializable = args.num_non_serializable

    def print_banner(self):
        print("Generating serializability exercise with the following parameters:")
        print(f"Number of conflict-serializable schedules: {self.num_serializable}")
        print(f"Number of non-conflict-serializable schedules: {self.num_non_serializable}")
        super().print_banner()

    def _generate_schedules_by_serializability(
        self,
        count: int,
        serializable: bool,
        max_attempts: int = None
    ) -> list[Schedule]:
        if count <= 0:
            return []

        if max_attempts is None:
            max_attempts = count * 300

        schedules = []
        seen = set()
        attempts = 0

        while len(schedules) < count and attempts < max_attempts:
            attempts += 1

            graph = ScheduleGenerator.generate_random_precedence_graph(
                transaction_count=self.num_transactions,
                edge_count=self.num_operations,
                acyclic=serializable,
                cyclic=not serializable
            )

            candidate = ScheduleGenerator.generate_schedule_from_cyclic_precedence_graph(
                graph,
                must_read_written=self.must_read,
                must_write_read=self.must_write
            )

            candidate = self._random_conflict_equivalent_permutation(candidate)

            if candidate.is_conflict_serializable() != serializable:
                continue

            candidate_key = tuple(str(op) for op in candidate.operations)
            if candidate_key in seen:
                continue

            seen.add(candidate_key)
            schedules.append(candidate)

        if len(schedules) < count:
            label = "conflict-serializable" if serializable else "non-conflict-serializable"
            raise RuntimeError(
                f"Could not generate enough {label} schedules with the current settings"
            )

        return schedules

    def generate(self):
        print("Generating schedules...")

        self._setup_random()

        serializable_schedules = self._generate_schedules_by_serializability(
            self.num_serializable,
            serializable=True
        )
        non_serializable_schedules = self._generate_schedules_by_serializability(
            self.num_non_serializable,
            serializable=False
        )

        for schedule in serializable_schedules:
            if not schedule.is_conflict_serializable():
                raise RuntimeError(
                    "Verification failed: expected conflict-serializable schedule"
                )

        for schedule in non_serializable_schedules:
            if schedule.is_conflict_serializable():
                raise RuntimeError(
                    "Verification failed: expected non-conflict-serializable schedule"
                )

        random.shuffle(serializable_schedules)
        random.shuffle(non_serializable_schedules)

        generated = serializable_schedules + non_serializable_schedules
        random.shuffle(generated)

        for i, schedule in enumerate(generated, start=1):
            schedule.id = i

        serializable_ids = [schedule.id for schedule in serializable_schedules]
        non_serializable_ids = [schedule.id for schedule in non_serializable_schedules]

        print("Generated schedules:")
        for schedule in generated:
            self._print_schedule(schedule)

        if self.print_conflict_graphs:
            print("Conflict graphs:")
            for schedule in generated:
                self._print_conflict_graph(schedule)

        print("Solutions:")
        print(f"Conflict-serializable: {sorted(serializable_ids)}")
        print(f"Not conflict-serializable: {sorted(non_serializable_ids)}")