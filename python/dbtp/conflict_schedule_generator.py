import random
from typing import Optional
from .constants import Constants

from .schedule_generator import ScheduleGenerator
from .directed_graph import DirectedGraph, CyclicGraphError, Vertex, Edge
from .operation import Operation, OperationType
from .schedule import Schedule

class ConflictScheduleGenerator(ScheduleGenerator):
    """

    Variables:
    ----------

    must_read_written: bool
        If True, ensure each WRITE is preceded by a READ of the same item by the same
        transaction (default: False)
    must_write_read: bool
        If True, ensure each READ is followed by a WRITE of the same item by the same
        transaction (default: False)
    avoid_two_node_cycles: bool
        If True, random cyclic precedence graphs avoid mutual edge pairs like
        T1 -> T2 and T2 -> T1, forcing any generated cycle to involve at least
        three transactions. Default: False.
    random_item_reuse: bool
        If True, the item-reuse decision in _assign_edge_items becomes probabilistic:
        even when a safe candidate label exists, a new label may be generated instead
        (controlled by new_item_probability). Default: False.
    new_item_probability: float
        Probability in [0, 1] that a *new* data item label is generated for an edge
        instead of reusing an existing safe candidate. Only used when
        random_item_reuse is True. Default: 0.5.
    """

    def __init__(
        self,
        *args,
        must_read_written: bool = False,
        must_write_read: bool = False,
        avoid_two_node_cycles: bool = False,
        random_item_reuse: bool = False,
        new_item_probability: float = 0.5,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.__must_read_written = must_read_written
        self.__must_write_read = must_write_read
        self.__avoid_two_node_cycles = avoid_two_node_cycles
        self.__random_item_reuse = random_item_reuse
        self.__new_item_probability = new_item_probability

    def __get_must_read_written(self):
        return self.__must_read_written
    
    def __set_must_read_written(self, value):
        self.__must_read_written = value

    must_read_written = property(__get_must_read_written, __set_must_read_written)

    def __get_must_write_read(self):
        return self.__must_write_read
    
    def __set_must_write_read(self, value):
        self.__must_write_read = value

    must_write_read = property(__get_must_write_read, __set_must_write_read)

    def __get_random_item_reuse(self):
        return self.__random_item_reuse

    def __set_random_item_reuse(self, value):
        self.__random_item_reuse = value

    random_item_reuse = property(__get_random_item_reuse, __set_random_item_reuse)

    def __get_avoid_two_node_cycles(self):
        return self.__avoid_two_node_cycles

    def __set_avoid_two_node_cycles(self, value):
        self.__avoid_two_node_cycles = value

    avoid_two_node_cycles = property(__get_avoid_two_node_cycles, __set_avoid_two_node_cycles)

    def __get_new_item_probability(self):
        return self.__new_item_probability

    def __set_new_item_probability(self, value):
        if not (0.0 <= value <= 1.0):
            raise ValueError("new_item_probability must be in [0, 1]")
        self.__new_item_probability = value

    new_item_probability = property(__get_new_item_probability, __set_new_item_probability)

    def add_non_conflicting_operations(
        self,
        schedule: Schedule,
        count: int
    ) -> Schedule:
        """
        Extend a schedule with non-conflicting operations inserted at random positions.

        Each added operation uses a fresh data item not present in the original
        schedule, so it shares no item with any existing operation and therefore
        introduces no new edges to the conflict graph regardless of insertion point.

        Parameters:
        -----------
        schedule: Schedule
            The schedule to extend.
        count: int
            Number of non-conflicting operations to insert.

        Returns:
        --------
        A new Schedule with the same id and the additional operations interleaved
        at random positions. If count is 0 the original schedule is returned unchanged.
        """
        if count < 0:
            raise ValueError("count must be >= 0")

        if count == 0:
            return schedule

        used_items = {op.item for op in schedule.operations}
        transactions = list({op.tx for op in schedule.operations})
        if not transactions:
            return schedule

        # Collect enough fresh item names not already used in the schedule
        fresh_items: list[str] = []
        counter = 0
        while len(fresh_items) < count:
            name = self._next_generated_item_name(counter)
            counter += 1
            if name not in used_items:
                fresh_items.append(name)

        ops = list(schedule.operations)
        for item_name in fresh_items:
            tx = random.choice(transactions)
            op_type = random.choice([OperationType.READ, OperationType.WRITE])
            new_op = Operation(tx=tx, op=op_type, item=item_name)
            pos = random.randint(0, len(ops))
            ops.insert(pos, new_op)

        return Schedule(id=schedule.id, operations=ops)

    def _next_generated_item_name(self, item_counter: int) -> str:
        
        """
        Pick the next data item name as a label for a conflict graph edge.

        Parameters:
        -----------
        item_counter: int
            The number of items generated so far, used to determine the next name.

        Returns:
        --------
        A string representing the next data item name, using single letters for the first 26 items
        """

        if item_counter < len(Constants.LETTERS):
            return Constants.LETTERS[item_counter]
        return f"X{item_counter}"
    
    def _collect_induced_conflict_edges(
        self,
        operations: list[tuple[int, bool, str]]
    ) -> set[tuple[int, int]]:
        
        """
        Given a list of operations (tx_id, is_write, item), collect all induced conflict edges
        between transactions.

        Parameters:
        -----------
        operations: list[tuple[int, bool, str]]
            A list of operations, where each operation is a tuple (tx_id, is_write, item).

        Returns:
        --------
        A set of tuples representing the induced conflict edges between transactions.

        A conflict edge (tx1, tx2) exists if there is a pair of operations on the same item where
        at least one is a write, and the operations are from different transactions.

        This function is used to check if a data item name can be safely reused for multiple edges
        of the conflict graph without introducing new conflict edges that are not in the original graph.
        """

        induced_edges = set()
        for i in range(len(operations)):
            tx1, is_write1, item1 = operations[i]
            for j in range(i + 1, len(operations)):
                tx2, is_write2, item2 = operations[j]
                if tx1 == tx2 or item1 != item2:
                    continue
                if is_write1 or is_write2:
                    induced_edges.add((tx1, tx2))
        return induced_edges
    
    def _build_operations_for_assignment(
        self,
        graph: DirectedGraph,
        edge_items: dict[tuple[int, int], str],
        mode: str
    ) -> list[tuple[int, bool, str]]:
        
        """
        Build a list of operations for a given edge item assignment.

        Parameters:
        -----------
        graph: DirectedGraph
            The conflict graph for which we are building operations.
        edge_items: dict[tuple[int, int], str]
            A mapping from graph edges (source, target) to assigned data item names.
        mode: str
            The mode of operation, either "acyclic" or "cyclic".

        Returns:
        --------
        A list of operations, where each operation is a tuple (tx_id, is_write, item).

        Remarks:
        --------
        This function generates a sequence of operations (reads and writes) for each transaction
        based on the assigned edge items. The generated operations respect the constraints
        specified by the `must_read_written` and `must_write_read` flags.

        The difference between "acyclic" and "cyclic" modes is in how the operations are ordered.
        In "acyclic" mode, a topological ordering of the transactions is used to ensure that all
        operations of a transaction appear after the operations of its predecessors in the graph.
        In "cyclic" mode, operations are generated by iterating through the edges without a
        guaranteed global ordering, which may be necessary when the graph contains cycles.
        """


        operations: list[tuple[int, bool, str]] = []

        if mode == "acyclic":
            ordering = graph.topological_sort()

            reads_by_tx = {tx: set() for tx in graph.vertices}
            writes_by_tx = {tx: set() for tx in graph.vertices}

            incoming_by_tx = {tx: [] for tx in graph.vertices}
            for (source, target), item_name in edge_items.items():
                incoming_by_tx[target].append(item_name)

            for tx in ordering:
                incoming_items = sorted(incoming_by_tx[tx])
                outgoing_items = sorted(
                    edge_items[(tx, target)]
                    for target in graph.adjacency[tx]
                    if (tx, target) in edge_items
                )

                for item_name in sorted(incoming_by_tx[tx]):
                    operations.append((tx, False, item_name))  # READ
                    reads_by_tx[tx].add(item_name)

                if self.__must_write_read:
                    for item_name in incoming_items:
                        if item_name in outgoing_items or item_name in writes_by_tx[tx]:
                            continue
                        operations.append((tx, True, item_name))
                        writes_by_tx[tx].add(item_name)

                for item_name in outgoing_items:
                    if self.__must_read_written and item_name not in reads_by_tx[tx]:
                        operations.append((tx, False, item_name))
                        reads_by_tx[tx].add(item_name)
                    operations.append((tx, True, item_name))   # WRITE
                    writes_by_tx[tx].add(item_name)

            return operations

        if mode == "cyclic":
            reads_by_tx = {tx: set() for tx in graph.vertices}
            writes_by_tx = {tx: set() for tx in graph.vertices}

            for (source, target), item_name in sorted(edge_items.items()):
                if self.__must_read_written and item_name not in reads_by_tx[source]:
                    operations.append((source, False, item_name))
                    reads_by_tx[source].add(item_name)

                operations.append((source, True, item_name))   # WRITE
                writes_by_tx[source].add(item_name)

                operations.append((target, False, item_name))  # READ
                reads_by_tx[target].add(item_name)

                if self.__must_write_read and item_name not in writes_by_tx[target]:
                    operations.append((target, True, item_name))
                    writes_by_tx[target].add(item_name)

            return operations

        raise ValueError(f"Unsupported edge assignment mode: {mode}")
    
    def _assign_edge_items(
        self,
        graph: DirectedGraph, mode: str
    ) -> dict[tuple[int, int], str]:
        
        """
        Assign data item names to each edge in the conflict graph.
        
        Parameters:
        -----------
        graph: DirectedGraph
            The conflict graph for which we are assigning edge items.
        mode: str
            The mode of operation, either "acyclic" or "cyclic", which may affect how items can be reused.
        
        Returns:
        --------
        A dictionary mapping each edge (source, target) to a data item name (str).
        
        Remarks:
        --------
        The assignment tries to minimize the number of unique data items used across all edges.
        It does this by attempting to reuse previously assigned data items for new edges, while
        ensuring that the induced conflict edges from the operations do not introduce any new
        edges that are not in the original graph. If no safe reuse is possible, a new unique
        data item name is generated for the edge.

        When random_item_reuse is True, even when a safe candidate label is found the
        algorithm may randomly decide (with probability new_item_probability) to skip
        reuse and generate a fresh label instead. This introduces variety in the number
        of distinct data items across generated schedules.

        Safety guarantee for clique consistency: the safety check builds the full
        operation sequence for ALL currently assigned edges (using their actual labels)
        and verifies that the induced conflict graph is a subgraph of the original.
        This implicitly enforces that the edges sharing a candidate label always form
        a clique in the conflict graph — even when some clique edges were randomly
        assigned different labels in earlier iterations.
        """
        
        edge_items: dict[tuple[int, int], str] = {}
        item_counter = 0
        generated_items_in_order: list[str] = []

        graph_edges = set(graph.edges.keys())

        for source in graph.vertices:
            for target in graph.adjacency[source]:
                edge = graph.edges[(source, target)]

                if edge.label is not None:
                    # Preserve explicit labels from the graph.
                    edge_items[(source, target)] = edge.label
                    continue

                chosen_item = None

                # Decide whether to even attempt label reuse for this edge.
                # When random_item_reuse is enabled, skip reuse with probability
                # new_item_probability so that generated schedules contain more
                # variety in the number of distinct data items.
                attempt_reuse = not (
                    self.__random_item_reuse
                    and random.random() < self.__new_item_probability
                )

                if attempt_reuse:
                    # Try to reuse a previously generated label.
                    # The safety check builds the full operation sequence for ALL
                    # currently assigned edges and verifies that the induced
                    # conflict graph remains a subgraph of the original.  This
                    # guarantees that the edges sharing the candidate label form a
                    # valid clique — regardless of what labels other clique edges
                    # were randomly assigned in earlier iterations.
                    for candidate_item in generated_items_in_order:
                        tentative = edge_items.copy()
                        tentative[(source, target)] = candidate_item
                        is_safe = True
                        for must_read_written in (False, True):
                            for must_write_read in (False, True):
                                operations = self._build_operations_for_assignment(
                                    graph,
                                    tentative,
                                    mode
                                )
                                induced_edges = self._collect_induced_conflict_edges(operations)
                                if not induced_edges.issubset(graph_edges):
                                    is_safe = False
                                    break
                            if not is_safe:
                                break

                        if is_safe:
                            chosen_item = candidate_item
                            break

                # If no safe reuse exists (or reuse was randomly skipped), create a new item.
                if chosen_item is None:
                    chosen_item = self._next_generated_item_name(item_counter)
                    item_counter += 1
                    generated_items_in_order.append(chosen_item)

                edge_items[(source, target)] = chosen_item

        return edge_items
    
    def generate_random_precedence_graph(
        self,
        transaction_count: int = 4,
        edge_count: int = 4,
        acyclic: bool = True,
        cyclic: bool = False
    ) -> DirectedGraph:

        """
        Generate a random precedence graph with the specified number of transactions.
        
        Parameters:
        -----------
        transaction_count: int
            Number of transactions (vertices) in the graph
        edge_count: int
            Number of edges to add to the graph
        acyclic: bool
            If True, keep the graph acyclic
        cyclic: bool
            If True, force the graph to contain at least one cycle

        Returns:
        --------
        A DirectedGraph representing the precedence graph
        """

        return self._generate_random_directed_graph(
            node_count=transaction_count,
            edge_count=edge_count,
            acyclic=acyclic,
            cyclic=cyclic,
            allow_two_node_cycles=not self.__avoid_two_node_cycles,
            failure_message="Failed to generate acyclic graph within max attempts",
        )
    
    def generate_schedule_from_acyclic_precedence_graph(
        self,
        graph: DirectedGraph
    ) -> Schedule:
        
        """
        Generate a schedule that produces the given precedence graph.

        Parameters:
        -----------
        graph: DirectedGraph
            An acyclic precedence graph where vertices are transaction IDs and edges
            represent precedence

        Returns:
        --------
        A Schedule with read and write operations that produce the same precedence graph

        Remarks:
        --------
        The algorithm:
        1. For each edge (i, j) in the precedence graph, assign a unique data item X_ij
        2. Transaction i will WRITE to X_ij, and transaction j will READ from X_ij
        3. This creates a write-read conflict: i -> j in the precedence graph
        4. Operations are ordered using a linear ordering that respects edges where possible

        When must_read_written is True, every WRITE operation is preceded by a READ of the same item
        by the same transaction (unless that read was already added for an incoming edge).
        When must_write_read is True, every READ operation is followed by a WRITE of the same item
        by the same transaction (unless that write already exists for an outgoing edge).
        """
        operations = []

        # For each edge (i, j), assign a unique data item
        edge_items = self._assign_edge_items(graph, mode="acyclic")

        # Try topological sort, if it fails (cyclic), use vertex order as-is
        ordering = graph.topological_sort()

        # Track reads/writes already added per transaction to avoid duplicating ops
        reads_by_tx = {tx: set() for tx in graph.vertices}
        writes_by_tx = {tx: set() for tx in graph.vertices}

        # Generate operations based on ordering
        # For each transaction in order:
        #   1. First, READ items from incoming edges
        #   2. Optionally WRITE items for incoming reads (must_write_read)
        #   3. Then, WRITE items for outgoing edges (optionally preceded by a READ)
        for tx1 in ordering:
            # Collect all incoming edges to this transaction
            incoming_items = []
            for tx2 in graph.vertices:
                for target in graph.adjacency[tx2]:
                    if target == tx1:
                        item_name = edge_items[(tx2, tx1)]
                        incoming_items.append(item_name)

            # Add READ operations for incoming edges
            for item_name in sorted(incoming_items):  # Sort for deterministic output
                operations.append(Operation(tx=tx1, op=OperationType.READ, item=item_name))
                reads_by_tx[tx1].add(item_name)

            # Collect all outgoing edges from this transaction
            outgoing_items = []
            for target in graph.adjacency[tx1]:
                item_name = edge_items[(tx1, target)]
                outgoing_items.append(item_name)

            # If required, ensure reads are followed by writes of same item by same tx
            if self.__must_write_read:
                for item_name in sorted(incoming_items):
                    # If this transaction already writes this item as an outgoing item, skip
                    if item_name in outgoing_items or item_name in writes_by_tx[tx1]:
                        continue
                    # Append a write for the read item
                    operations.append(Operation(tx=tx1, op=OperationType.WRITE, item=item_name))
                    writes_by_tx[tx1].add(item_name)

            # Add WRITE operations for outgoing edges
            for item_name in sorted(outgoing_items):  # Sort for deterministic output
                if self.__must_read_written and item_name not in reads_by_tx[tx1]:
                    # Precede the write with a read of the same item by the same transaction
                    operations.append(Operation(tx=tx1, op=OperationType.READ, item=item_name))
                    reads_by_tx[tx1].add(item_name)
                operations.append(Operation(tx=tx1, op=OperationType.WRITE, item=item_name))
                writes_by_tx[tx1].add(item_name)

        return Schedule(id=1, operations=operations)
    
    def generate_schedule_from_cyclic_precedence_graph(
        self,
        graph: DirectedGraph
    ) -> Schedule:
        """
        Generate a schedule from a cyclic precedence graph by iterating edges.

        Parameters:
        -----------
        graph: DirectedGraph
            A directed graph (possibly cyclic) where vertices are transaction IDs

        Returns:
        --------
        A Schedule with operations that produce the given precedence graph

        Remarks:
        --------
        For each edge (i, j):
        - Transaction i WRITEs a unique data item X_ij
        - Transaction j READs the same item X_ij
        - This creates a write-read conflict: i -> j
        """
        operations = []
        
        # Track reads/writes per transaction
        reads_by_tx = {tx: set() for tx in graph.vertices}
        writes_by_tx = {tx: set() for tx in graph.vertices}
        
        # Assign unique data items to each edge
        edge_items = self._assign_edge_items(graph, mode="cyclic")
        
        # Iterate through edges and add operations
        for (source, target), item_name in sorted(edge_items.items()):
            # Add WRITE for source transaction
            if self.__must_read_written and item_name not in reads_by_tx[source]:
                operations.append(Operation(tx=source, op=OperationType.READ, item=item_name))
                reads_by_tx[source].add(item_name)
            
            operations.append(Operation(tx=source, op=OperationType.WRITE, item=item_name))
            writes_by_tx[source].add(item_name)
            
            # Add READ for target transaction
            operations.append(Operation(tx=target, op=OperationType.READ, item=item_name))
            reads_by_tx[target].add(item_name)
            
            # Add WRITE for target if must_write_read is enabled
            if self.__must_write_read and item_name not in writes_by_tx[target]:
                operations.append(Operation(tx=target, op=OperationType.WRITE, item=item_name))
                writes_by_tx[target].add(item_name)
        
        return Schedule(id=1, operations=operations)
    
    def generate_conflict_equivalent_permutations(
        self,
        schedule: Schedule,
        max_permutations: Optional[int] = None
    ) -> list[Schedule]:
        """
        Generate all conflict-equivalent permutations of the given schedule.

        Parameters:
        -----------
        schedule: Schedule
            The original schedule to permute
        max_permutations: Optional[int]
            If provided, stop after generating this many permutations

        Returns:
        --------
        A list of Schedules that are conflict-equivalent to the input schedule
        """
        if max_permutations is not None and max_permutations <= 0:
            return []

        ops = schedule.operations
        n = len(ops)

        adjacency, indegree = self._build_permutation_dependencies(schedule)

        results: list[Schedule] = []

        # Backtracking to enumerate all topological sorts of the partial order
        def backtrack(path: list[int], indeg: dict[int, int], available: list[int]):
            # Early stop if we've reached the requested number of permutations
            if max_permutations is not None and len(results) >= max_permutations:
                return

            if len(path) == n:
                perm_ops = [ops[i] for i in path]
                # Create fresh Operation instances to avoid mutating originals
                perm_ops_copied = [
                    Operation(tx=o.tx, op=o.op, item=o.item) for o in perm_ops
                ]
                results.append(Schedule(id=schedule.id, operations=perm_ops_copied))
                return

            # Iterate over a snapshot of available nodes in deterministic order
            for idx in sorted(available):
                # Early stop check before choosing next
                if max_permutations is not None and len(results) >= max_permutations:
                    return

                # Choose idx
                path.append(idx)
                new_available = available.copy()
                new_available.remove(idx)
                indeg_snapshot = indeg.copy()

                # Decrease indegree of neighbors and add newly available nodes
                for neigh in adjacency[idx]:
                    indeg_snapshot[neigh] -= 1
                    if indeg_snapshot[neigh] == 0:
                        new_available.append(neigh)

                backtrack(path, indeg_snapshot, new_available)

                # Backtrack
                path.pop()

                # Another early stop after backtracking
                if max_permutations is not None and len(results) >= max_permutations:
                    return

        initial_available = [i for i in range(n) if indegree[i] == 0]
        backtrack([], indegree, initial_available)
        
        return results

    def _build_permutation_dependencies(
        self,
        schedule: Schedule
    ) -> tuple[dict[int, list[int]], dict[int, int]]:

        """
        Build DAG dependencies for schedule permutations.

        Dependencies are the union of:
        1. Conflict edges between operations.
        2. In-transaction order edges between consecutive operations of the same
           transaction, so each transaction keeps its exact operation order.
        """

        graph = schedule.build_conflict_graph()
        adjacency = {
            node: set(neighbors)
            for node, neighbors in graph.adjacency.items()
        }

        indegree = {node: 0 for node in graph.vertices.keys()}
        for source in adjacency.keys():
            for target in adjacency[source]:
                indegree[target] += 1

        last_index_by_tx: dict[int, int] = {}
        for index, op in enumerate(schedule.operations):
            if op.tx in last_index_by_tx:
                previous = last_index_by_tx[op.tx]
                if index not in adjacency[previous]:
                    adjacency[previous].add(index)
                    indegree[index] += 1
            last_index_by_tx[op.tx] = index

        return {
            node: sorted(neighbors)
            for node, neighbors in adjacency.items()
        }, indegree
    
    def generate_random_conflict_equivalent_permutations(
        self,
        schedule: Schedule,
        count: int = 10,
    ) -> list[Schedule]:
        """
        Generate random conflict-equivalent permutations efficiently.
        
        Instead of enumerating all permutations (which can be exponential),
        this method randomly samples topological sorts of the conflict graph.
        This is much faster when there are many possible permutations.
        
        Parameters:
        -----------
        schedule: Schedule
            The original schedule to permute
        count: int
            Number of random permutations to generate

        Returns:
        --------
        A list of up to 'count' unique random conflict-equivalent schedules
        """

        if count <= 0:
            return []
        
        ops = schedule.operations
        n = len(ops)
        
        if n == 0:
            return []

        adjacency, base_indegree = self._build_permutation_dependencies(schedule)
        
        results: list[Schedule] = []
        seen_permutations: set[tuple[int, ...]] = set()
        
        max_attempts = count * 100 if self.max_attempts is None else self.max_attempts
        
        attempts = 0
        
        while len(results) < count and attempts < max_attempts:
            attempts += 1
            
            # Generate one random topological sort
            indegree = base_indegree.copy()
            available = [i for i in range(n) if indegree[i] == 0]
            permutation = []
            
            while available:
                # Randomly choose from available nodes
                idx = random.choice(available)
                permutation.append(idx)
                available.remove(idx)
                
                # Update indegrees and available nodes
                for neighbor in adjacency[idx]:
                    indegree[neighbor] -= 1
                    if indegree[neighbor] == 0:
                        available.append(neighbor)
            
            # Check if this permutation is unique
            perm_tuple = tuple(permutation)
            if perm_tuple not in seen_permutations:
                seen_permutations.add(perm_tuple)
                
                # Create the schedule from this permutation
                perm_ops = [ops[i] for i in permutation]
                perm_ops_copied = [
                    Operation(tx=o.tx, op=o.op, item=o.item) for o in perm_ops
                ]
                results.append(Schedule(id=schedule.id, operations=perm_ops_copied))
        
        return results