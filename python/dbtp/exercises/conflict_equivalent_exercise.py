import random

from .conflict_exercise import ConflictExercise
from ..schedule_generator import ScheduleGenerator


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

        schedule = self._generate_reference_schedule()
        schedules = [schedule]

        equivalents = ScheduleGenerator.generate_random_conflict_equivalent_permutations(
            schedule,
            count=self.num_schedules - 1
        )
        schedules.extend(equivalents)

        random.shuffle(schedules)

        for i in range(len(schedules)):
            schedules[i].id = i + 1

        print("Generated schedules:")

        for schedule in schedules:
            self._print_schedule(schedule)

        if self.print_conflict_graphs:
            print("Conflict graphs:")

            for schedule in schedules:
                self._print_conflict_graph(schedule)