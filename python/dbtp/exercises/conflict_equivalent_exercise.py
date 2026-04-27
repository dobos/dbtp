import random

from .conflict_exercise import ConflictExercise


class ConflictEquivalentExercise(ConflictExercise):
    def __init__(self):
        super().__init__()
        self.num_schedules = 4

    @staticmethod
    def create_parser(subparsers):
        parser = subparsers.add_parser(
            "conf-eq",
            help="Generate conflict-equivalency exercise"
        )
        parser.add_argument(
            "--num-schedules",
            type=int,
            help="Number of schedules to generate"
        )
        ConflictEquivalentExercise._add_common_arguments(parser)

    def parse_args(self, args):
        super().parse_args(args)
        if args.num_schedules is not None:
            self.num_schedules = args.num_schedules

    def print_banner(self):
        print("Generating conflict-equivalency exercise with the following parameters:")
        print(f"Number of schedules: {self.num_schedules}")
        super().print_banner()

    def generate(self):
        print("Generating schedules...")

        self._setup_random()

        schedule = self._random_conflict_equivalent_permutation(
            self._generate_reference_schedule()
        )
        schedules = [schedule]

        equivalents = self._generate_conflict_equivalent_schedules(
            schedule,
            self.num_schedules - 1
        )
        schedules.extend(equivalents)

        # Permute each equivalent schedule once more for varied operation ordering.
        schedules = [self._random_conflict_equivalent_permutation(s) for s in schedules]

        random.shuffle(schedules)

        for i in range(len(schedules)):
            schedules[i].id = i + 1

        self._print_schedules(schedules)

        if self.print_conflict_graphs:
            self._print_conflict_graphs(schedules)