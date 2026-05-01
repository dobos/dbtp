import random

from ..directedgraph import DirectedGraph
from ..schedule import Schedule
from ..schedulegenerator import ScheduleGenerator


class DeadlockExercise:
    def __init__(self):
        self.num_transactions = 4
        self.num_operations = 4
        self.seed = None
        self.deadlocking = True
        self.latex = False
        self.print_wait_for_graph = False
        self.graph_after_ops = None

    @staticmethod
    def create_parser(subparsers):
        parser = subparsers.add_parser(
            "deadlock",
            help="Generate strict-2PL schedules with or without deadlock"
        )
        parser.add_argument(
            "--num-transactions",
            type=int,
            help="Number of transactions in the schedule"
        )
        parser.add_argument(
            "--num-operations",
            type=int,
            help="Number of wait-for edges to generate"
        )
        parser.add_argument(
            "--seed",
            type=int,
            help="Random seed for reproducible generation"
        )

        deadlock_group = parser.add_mutually_exclusive_group()
        deadlock_group.add_argument(
            "--deadlocking",
            action="store_true",
            dest="deadlocking",
            default=True,
            help="Generate a schedule that deadlocks"
        )
        deadlock_group.add_argument(
            "--non-deadlocking",
            action="store_false",
            dest="deadlocking",
            help="Generate a schedule that does not deadlock"
        )

        parser.add_argument(
            "--graph",
            action="store_true",
            dest="print_wait_for_graph",
            help="Print wait-for graph"
        )
        parser.add_argument(
            "--graph-after-ops",
            type=int,
            help="Print wait-for graph after this many operations"
        )
        parser.add_argument(
            "--latex",
            action="store_true",
            dest="latex",
            help="Output schedule and graph in LaTeX/TikZ format"
        )

    def parse_args(self, args):
        if args.num_transactions is not None:
            self.num_transactions = args.num_transactions
        if args.num_operations is not None:
            self.num_operations = args.num_operations
        if hasattr(args, "seed") and args.seed is not None:
            self.seed = args.seed
        if hasattr(args, "deadlocking") and args.deadlocking is not None:
            self.deadlocking = args.deadlocking
        if hasattr(args, "latex") and args.latex is not None:
            self.latex = args.latex
        if hasattr(args, "print_wait_for_graph") and args.print_wait_for_graph is not None:
            self.print_wait_for_graph = args.print_wait_for_graph
        if hasattr(args, "graph_after_ops") and args.graph_after_ops is not None:
            self.graph_after_ops = args.graph_after_ops

    def print_banner(self):
        print("Generating strict-2PL deadlock exercise with the following parameters:")
        print(f"Number of transactions: {self.num_transactions}")
        print(f"Number of wait-for edges: {self.num_operations}")
        print(f"Random seed: {self.seed}")
        print(f"Target deadlocking: {self.deadlocking}")
        print(f"Print wait-for graph: {self.print_wait_for_graph}")
        print(f"Graph after ops: {self.graph_after_ops}")

    def _setup_random(self):
        if self.seed is not None:
            random.seed(self.seed)

    def _generate_schedule(self) -> Schedule:
        wait_for_graph = ScheduleGenerator.generate_random_wait_for_graph(
            transaction_count=self.num_transactions,
            edge_count=self.num_operations,
            acyclic=not self.deadlocking,
            cyclic=self.deadlocking,
        )

        schedule = ScheduleGenerator.generate_schedule_from_wait_for_graph(wait_for_graph)
        schedule.id = 1

        # Verify strict-2PL and legality constraints.
        if not schedule.is_two_phase_locked():
            raise RuntimeError("Generated schedule is not two-phase locked")
        if not schedule.is_legal():
            raise RuntimeError("Generated schedule is not legal")

        has_deadlock = schedule.has_deadlock()
        if has_deadlock != self.deadlocking:
            raise RuntimeError(
                "Generated schedule does not match deadlocking target"
            )

        return schedule

    @staticmethod
    def wait_for_graph_after_ops(
        schedule: Schedule,
        op_count: int | None = None
    ) -> DirectedGraph:
        if op_count is None:
            prefix_ops = schedule.operations
        else:
            prefix_ops = schedule.operations[:max(0, min(op_count, len(schedule.operations)))]

        prefix_schedule = Schedule(id=schedule.id, operations=prefix_ops)
        return prefix_schedule.build_wait_for_graph()

    def _print_schedule(self, schedule: Schedule):
        if self.latex:
            print(schedule.latex())
        else:
            print(str(schedule))

    def _print_wait_for_graph(self, schedule: Schedule):
        graph = self.wait_for_graph_after_ops(schedule, self.graph_after_ops)

        if self.graph_after_ops is None:
            print(f"Wait-for graph for Schedule S_{schedule.id} (all operations):")
        else:
            print(
                f"Wait-for graph for Schedule S_{schedule.id} "
                f"after {self.graph_after_ops} operations:"
            )

        if self.latex:
            print(graph.latex())
        else:
            print(str(graph))

    def generate(self):
        print("Generating schedule...")

        self._setup_random()
        schedule = self._generate_schedule()

        print("Generated schedule:")
        self._print_schedule(schedule)

        if self.print_wait_for_graph:
            self._print_wait_for_graph(schedule)

        print("Solution:")
        print(f"Deadlock occurs: {schedule.has_deadlock()}")
