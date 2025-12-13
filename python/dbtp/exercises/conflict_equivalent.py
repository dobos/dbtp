import random

from ..schedule_generator import ScheduleGenerator

class ConflictEquivalentExercise:
    def __init__(self):
        self.num_schedules = 4
        self.num_transactions = 4
        self.num_operations = 4
        self.must_read = True
        self.must_write = False
        self.serializable = False
        self.latex = False

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
        parser.add_argument(
            "--num-transactions",
            type=int,
            help="Number of transactions in the schedule"
        )
        parser.add_argument(
            "--num-operations",
            type=int,
            help="Number of conflicting operations"
        )
        parser.add_argument(
            "--must-read",
            action="store_true",
            dest="must_read",
            help="Must include read operations before any write operations"
        )
        parser.add_argument(
            "--no-must-read",
            action="store_false",
            dest="must_read",
            help="Do not require read operations before write operations"
        )
        parser.add_argument(
            "--must-write",
            action="store_true",
            dest="must_write",
            help="Must include write operations after read operations"
        )
        parser.add_argument(
            "--no-must-write",
            action="store_false",
            dest="must_write",
            help="Do not require write operations after read operations"
        )
        parser.add_argument(
            "--serializable",
            action="store_true",
            dest="serializable",
            help="Generate only serializable schedules"
        )
        parser.add_argument(
            "--no-serializable",
            action="store_false",
            dest="serializable",
            help="Allow non-serializable schedules"
        )
        parser.add_argument(
            "--latex",
            action="store_true",
            dest="latex",
            help="Output schedules in LaTeX format"
        )

    def parse_args(self, args):
        if args.num_schedules is not None:
            self.num_schedules = args.num_schedules
        if args.num_transactions is not None:
            self.num_transactions = args.num_transactions
        if args.num_operations is not None:
            self.num_operations = args.num_operations
        if hasattr(args, "must_read") and args.must_read is not None:
            self.must_read = args.must_read
        if hasattr(args, "must_write") and args.must_write is not None:
            self.must_write = args.must_write
        if hasattr(args, "serializable") and args.serializable is not None:
            self.serializable = args.serializable
        if hasattr(args, "latex") and args.latex is not None:
            self.latex = args.latex

    def print_banner(self):
        print("Generating conflict-equivalency exercise with the following parameters:")
        print(f"Number of schedules: {self.num_schedules}")
        print(f"Number of transactions: {self.num_transactions}")
        print(f"Number of conflicting operations: {self.num_operations}")
        print(f"Must read before write: {self.must_read}")
        print(f"Must write after read: {self.must_write}")

    def generate(self):
        print("Generating schedules...")

        schedules = []
        
        # Generate a random precedence graph
        graph = ScheduleGenerator.generate_random_precedence_graph(
            transaction_count = self.num_transactions,
            edge_count = self.num_operations,
            acyclic = self.serializable,
            cyclic = not self.serializable
        )

        # Generate a schedule from the graph
        schedule = ScheduleGenerator.generate_schedule_from_cyclic_precedence_graph(
            graph,
            must_read_written=self.must_read,
            must_write_read=self.must_write
        )
        schedules.append(schedule)

        equivalents = ScheduleGenerator.generate_random_conflict_equivalent_permutations(
            schedule,
            count=self.num_schedules - 1
        )
        schedules.extend(equivalents)

        # Randomize the list of schedules
        random.shuffle(schedules)

        # Add numbers
        for i in range(len(schedules)):
            schedules[i].id = i + 1

        print("Generated schedules:")

        for schedule in schedules:
            if self.latex:
                print(schedule.latex())
            else:
                print(str(schedule))

        print("Conflict graphs:")

        for schedule in schedules:
            graph = schedule.build_precedence_graph()
            print(f"Conflict graph for Schedule S_{schedule.id}:")
            if self.latex:
                print(graph.latex())
            else:
                print(str(graph))