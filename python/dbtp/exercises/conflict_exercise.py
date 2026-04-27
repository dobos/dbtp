import random

from ..schedule import Schedule
from ..schedule_generator import ScheduleGenerator


class ConflictExercise:
    def __init__(self):
        self.num_transactions = 4
        self.num_operations = 4
        self.seed = None
        self.must_read = True
        self.must_write = False
        self.serializable = False
        self.latex = False
        self.print_conflict_graphs = False

    @staticmethod
    def _add_common_arguments(parser):
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
            "--seed",
            type=int,
            help="Random seed for reproducible generation"
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
        parser.add_argument(
            "--graph",
            action="store_true",
            dest="print_conflict_graphs",
            help="Print conflict graphs for generated schedules"
        )

    def parse_args(self, args):
        if args.num_transactions is not None:
            self.num_transactions = args.num_transactions
        if args.num_operations is not None:
            self.num_operations = args.num_operations
        if hasattr(args, "seed") and args.seed is not None:
            self.seed = args.seed
        if hasattr(args, "must_read") and args.must_read is not None:
            self.must_read = args.must_read
        if hasattr(args, "must_write") and args.must_write is not None:
            self.must_write = args.must_write
        if hasattr(args, "serializable") and args.serializable is not None:
            self.serializable = args.serializable
        if hasattr(args, "latex") and args.latex is not None:
            self.latex = args.latex
        if hasattr(args, "print_conflict_graphs") and args.print_conflict_graphs is not None:
            self.print_conflict_graphs = args.print_conflict_graphs

    def print_banner(self):
        print(f"Number of transactions: {self.num_transactions}")
        print(f"Number of conflicting operations: {self.num_operations}")
        print(f"Random seed: {self.seed}")
        print(f"Must read before write: {self.must_read}")
        print(f"Must write after read: {self.must_write}")
        print(f"Print conflict graphs: {self.print_conflict_graphs}")

    def _setup_random(self):
        if self.seed is not None:
            random.seed(self.seed)

    def _generate_reference_schedule(self) -> Schedule:
        graph = ScheduleGenerator.generate_random_precedence_graph(
            transaction_count=self.num_transactions,
            edge_count=self.num_operations,
            acyclic=self.serializable,
            cyclic=not self.serializable
        )
        return ScheduleGenerator.generate_schedule_from_cyclic_precedence_graph(
            graph,
            must_read_written=self.must_read,
            must_write_read=self.must_write
        )

    def _print_schedule(self, schedule: Schedule):
        if self.latex:
            print(schedule.latex())
        else:
            print(str(schedule))

    def _print_conflict_graph(self, schedule: Schedule):
        graph = schedule.build_precedence_graph()
        print(f"Conflict graph for Schedule S_{schedule.id}:")
        if self.latex:
            print(graph.latex())
        else:
            print(str(graph))
