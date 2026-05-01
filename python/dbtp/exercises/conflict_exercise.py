import random

from ..schedule import Schedule
from ..conflict_schedule_generator import ConflictScheduleGenerator


class ConflictExercise:
    def __init__(self):
        self.num_transactions = 4
        self.num_operations = 4
        self.seed = None
        self.must_read = True
        self.must_write = False
        self.serializable = False
        self.allow_two_node_cycles = False
        self.random_permutation = True
        self.random_item_reuse = False
        self.new_item_probability = 0.33
        self.num_non_conflicting_operations = 0
        self.latex = False
        self.print_conflict_graphs = False

    @staticmethod
    def _add_common_arguments(parser, include_serializable: bool = True):
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
        if include_serializable:
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
            "--allow-two-node-cycles",
            action="store_true",
            dest="allow_two_node_cycles",
            default=False,
            help="Allow trivial two-transaction cycles in cyclic precedence graphs"
        )
        parser.add_argument(
            "--no-allow-two-node-cycles",
            action="store_false",
            dest="allow_two_node_cycles",
            help="Require cyclic precedence graphs to use at least three transactions"
        )
        parser.add_argument(
            "--random-permutation",
            action="store_true",
            dest="random_permutation",
            default=True,
            help="Enable random conflict-equivalent operation permutations"
        )
        parser.add_argument(
            "--no-random-permutation",
            action="store_false",
            dest="random_permutation",
            help="Disable random conflict-equivalent operation permutations"
        )
        parser.add_argument(
            "--random-item-reuse",
            action="store_true",
            dest="random_item_reuse",
            help="Randomly reuse data items across operations"
        )
        parser.add_argument(
            "--new-item-probability",
            type=float,
            dest="new_item_probability",
            help="Probability of introducing a new data item (0.0-1.0, default 0.5)"
        )
        parser.add_argument(
            "--num-non-conflicting-operations",
            type=int,
            dest="num_non_conflicting_operations",
            help="Number of non-conflicting operations to insert at random positions (default 0)"
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
        if hasattr(args, "allow_two_node_cycles") and args.allow_two_node_cycles is not None:
            self.allow_two_node_cycles = args.allow_two_node_cycles
        if hasattr(args, "random_permutation") and args.random_permutation is not None:
            self.random_permutation = args.random_permutation
        if hasattr(args, "random_item_reuse") and args.random_item_reuse is not None:
            self.random_item_reuse = args.random_item_reuse
        if hasattr(args, "new_item_probability") and args.new_item_probability is not None:
            self.new_item_probability = args.new_item_probability
        if hasattr(args, "num_non_conflicting_operations") and args.num_non_conflicting_operations is not None:
            self.num_non_conflicting_operations = args.num_non_conflicting_operations
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
        print(f"Allow two-node cycles: {self.allow_two_node_cycles}")
        print(f"Random conflict-equivalent permutation: {self.random_permutation}")
        print(f"Random item reuse: {self.random_item_reuse}")
        print(f"New item probability: {self.new_item_probability}")
        print(f"Non-conflicting operations: {self.num_non_conflicting_operations}")
        print(f"Print conflict graphs: {self.print_conflict_graphs}")

    def _setup_random(self):
        if self.seed is not None:
            random.seed(self.seed)

    def _generate_reference_schedule(self) -> Schedule:
        avoid_two_node_cycles = (
            not self.allow_two_node_cycles
            and (
                self.serializable
                or (self.num_transactions >= 3 and self.num_operations >= 3)
            )
        )

        generator = ConflictScheduleGenerator(
            must_read_written=self.must_read,
            must_write_read=self.must_write,
            avoid_two_node_cycles=avoid_two_node_cycles,
            random_item_reuse=self.random_item_reuse,
            new_item_probability=self.new_item_probability
        )

        graph = generator.generate_random_precedence_graph(
            transaction_count=self.num_transactions,
            edge_count=self.num_operations,
            acyclic=self.serializable,
            cyclic=not self.serializable
        )
        schedule = generator.generate_schedule_from_cyclic_precedence_graph(
            graph,
        )
        if self.num_non_conflicting_operations > 0:
            schedule = generator.add_non_conflicting_operations(
                schedule,
                self.num_non_conflicting_operations
            )
        return schedule

    def _random_conflict_equivalent_permutation(
        self,
        schedule: Schedule,
        count: int = 10,
        max_attempts: int = 600
    ) -> Schedule:
        if not self.random_permutation:
            return schedule
        
        generator = ConflictScheduleGenerator(
            max_attempts=max_attempts
        )

        permutations = generator.generate_random_conflict_equivalent_permutations(
            schedule,
            count=count,
        )
        if not permutations:
            return schedule

        original_key = tuple(str(op) for op in schedule.operations)
        different = [
            perm
            for perm in permutations
            if tuple(str(op) for op in perm.operations) != original_key
        ]
        if different:
            return random.choice(different)
        return permutations[0]

    def _generate_conflict_equivalent_schedules(
        self,
        reference: Schedule,
        count: int,
        max_attempts: int = None
    ) -> list[Schedule]:
        if count <= 0:
            return []

        if max_attempts is None:
            max_attempts = max(count * 300, 1000)

        generator = ConflictScheduleGenerator(
            max_attempts=max_attempts
        )

        equivalent = generator.generate_random_conflict_equivalent_permutations(
            reference,
            count=count,
        )

        if len(equivalent) < count:
            raise RuntimeError(
                "Could not generate enough conflict-equivalent schedules with the current settings"
            )

        return equivalent

    def _print_schedules(self, schedules: list[Schedule], heading: str = "Generated schedules:"):
        print(heading)
        for schedule in schedules:
            self._print_schedule(schedule)

    def _print_conflict_graphs(self, schedules: list[Schedule], heading: str | None = "Conflict graphs:"):
        if heading:
            print(heading)
        for schedule in schedules:
            self._print_conflict_graph(schedule)

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
