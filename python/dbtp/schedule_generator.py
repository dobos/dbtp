import random
from typing import Optional
from .constants import Constants
from .directedgraph import DirectedGraph, CyclicGraphError, Vertex, Edge
from .operation import Operation, OperationType
from .schedule import Schedule


class ScheduleGenerator:
    @classmethod
    def generate_random_precedence_graph(
        cls,
        transaction_count: int = 4,
        edge_count: int = 4,
        acyclic: bool = True,
        max_attempts: int = None
    ) -> DirectedGraph:

        """
        Generate a random precedence graph with the specified number of transactions.
        Args:
            transaction_count: Number of transactions (vertices) in the graph
            edge_count: Number of edges to add to the graph
            max_attempts: Maximum attempts to generate the desired graph structure
        Returns:
            A DirectedGraph representing the precedence graph
        """

        vertices = [ Vertex(id=i, label=i) for i in range(1, transaction_count + 1) ]
        graph = DirectedGraph(vertices=vertices)

        # Randomly add directed edges while ensuring acyclicity/cyclicity constraint
        added_edges = 0
        max_attempts = edge_count * 20 if max_attempts is None else max_attempts  # Prevent infinite loops
        attempts = 0
        
        while added_edges < edge_count and attempts < max_attempts:
            attempts += 1
            src = random.randint(1, transaction_count)
            dst = random.randint(1, transaction_count)
            
            if src == dst:
                continue

            edge = Edge(source=src, target=dst)
            
            # Check if edge already exists
            if graph.has_edge(edge):
                continue
            
            # Temporarily add edge and check for cycles
            graph.add_edge(edge)
            
            if acyclic:
                try:
                    graph.topological_sort()
                    # Graph is still acyclic, keep the edge
                    added_edges += 1
                except CyclicGraphError:
                    # Graph became cyclic, remove the edge
                    graph.remove_edge(edge)
            else:
                # We want any type of graph, keep the edge
                added_edges += 1

        if attempts == max_attempts:
            raise RuntimeError(f"Failed to generate acyclic graph within max attempts")

        return graph
    
    @classmethod
    def generate_random_cyclic_precedence_graph(
        cls,
        transaction_count: int = 4,
        edge_count: int = 4,
        max_attempts: int = None
    ) -> DirectedGraph:
        
        """
        Generate a random cyclic precedence graph.
        
        Strategy: First create a cycle to guarantee the graph is cyclic,
        then add remaining edges randomly.
        
        Args:
            transaction_count: Number of transactions (vertices) in the graph
            edge_count: Number of edges to add to the graph
            max_attempts: Maximum attempts to add random edges after the cycle
            
        Returns:
            A DirectedGraph that is guaranteed to be cyclic
        """

        if transaction_count < 2:
            raise ValueError("Need at least 2 transactions to create a cyclic graph")
        
        if edge_count < 2:
            raise ValueError("Need at least 2 edges to create a cycle")
        
        vertices = [Vertex(id=i, label=i) for i in range(1, transaction_count + 1)]
        graph = DirectedGraph(vertices=vertices)
        
        # Step 1: Create a random cycle to guarantee cyclicity
        # Choose a random subset of vertices for the cycle (at least 2)
        cycle_length = min(random.randint(2, transaction_count), edge_count)
        cycle_vertices = random.sample(range(1, transaction_count + 1), cycle_length)
        
        # Add edges to form a cycle
        for i in range(cycle_length):
            src = cycle_vertices[i]
            dst = cycle_vertices[(i + 1) % cycle_length]
            graph.add_edge(Edge(source=src, target=dst))
        
        added_edges = cycle_length
        
        # Step 2: Add remaining edges randomly
        max_attempts = (edge_count - added_edges) * 20 if max_attempts is None else max_attempts
        attempts = 0
        
        while added_edges < edge_count and attempts < max_attempts:
            attempts += 1
            src = random.randint(1, transaction_count)
            dst = random.randint(1, transaction_count)
            
            if src == dst:
                continue
            
            edge = Edge(source=src, target=dst)
            
            # Check if edge already exists
            if graph.has_edge(edge):
                continue
            
            # Add the edge (no need to check for cycles since we want cyclic graphs)
            graph.add_edge(edge)
            added_edges += 1
            
        if attempts == max_attempts and added_edges < edge_count:
            raise RuntimeError(f"Failed to generate cyclic graph with {edge_count} edges within max attempts")
        
        return graph

    @classmethod
    def generate_from_precedence_graph(
        cls,
        graph: DirectedGraph,
        must_read_written: bool = False,
        must_write_read: bool = False
    ) -> Schedule:
        
        """
        Generate a schedule that produces the given precedence graph.

        The algorithm:
        1. For each edge (i, j) in the precedence graph, assign a unique data item X_ij
        2. Transaction i will WRITE to X_ij, and transaction j will READ from X_ij
        3. This creates a write-read conflict: i -> j in the precedence graph
        4. Operations are ordered using topological sort to ensure precedence is maintained

        When must_read_written is True, every WRITE operation is preceded by a READ of the same item
        by the same transaction (unless that read was already added for an incoming edge).
        When must_write_read is True, every READ operation is followed by a WRITE of the same item
        by the same transaction (unless that write already exists for an outgoing edge).

        Args:
            graph: A directed graph where vertices are transaction IDs and edges represent precedence
            must_read_written: If True, ensure each WRITE is preceded by a READ of the same item by the same tx
            must_write_read: If True, ensure each READ is followed by a WRITE of the same item by the same tx

        Returns:
            A Schedule with read and write operations that produce the same precedence graph
        """
        operations = []

        # For each edge (i, j), assign a unique data item
        edge_items = {}
        item_counter = 0

        for source in graph.vertices:
            for target in graph.adjacency[source]:
                edge = graph.edges[(source, target)]
                if edge.label is None:
                    # Create a unique data item for this edge
                    item_name = f"{Constants.LETTERS[item_counter]}"
                else:
                    item_name = edge.label
                edge_items[(source, target)] = item_name
                item_counter += 1

        topo_order = graph.topological_sort()

        # Track reads/writes already added per transaction to avoid duplicating ops
        reads_by_tx = {tx: set() for tx in graph.vertices}
        writes_by_tx = {tx: set() for tx in graph.vertices}

        # Generate operations based on topological order
        # For each transaction in topological order:
        #   1. First, READ items from incoming edges
        #   2. Optionally WRITE items for incoming reads (must_write_read)
        #   3. Then, WRITE items for outgoing edges (optionally preceded by a READ)
        for tx1 in topo_order:
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
            if must_write_read:
                for item_name in sorted(incoming_items):
                    # If this transaction already writes this item as an outgoing item, skip
                    if item_name in outgoing_items or item_name in writes_by_tx[tx1]:
                        continue
                    # Append a write for the read item
                    operations.append(Operation(tx=tx1, op=OperationType.WRITE, item=item_name))
                    writes_by_tx[tx1].add(item_name)

            # Add WRITE operations for outgoing edges
            for item_name in sorted(outgoing_items):  # Sort for deterministic output
                if must_read_written and item_name not in reads_by_tx[tx1]:
                    # Precede the write with a read of the same item by the same transaction
                    operations.append(Operation(tx=tx1, op=OperationType.READ, item=item_name))
                    reads_by_tx[tx1].add(item_name)
                operations.append(Operation(tx=tx1, op=OperationType.WRITE, item=item_name))
                writes_by_tx[tx1].add(item_name)

        return Schedule(id=1, operations=operations)

    @classmethod
    def generate_conflict_equivalent_permutations(
        cls,
        schedule: Schedule,
        max_permutations: Optional[int] = None
    ) -> list[Schedule]:
        """
        Generate all conflict-equivalent permutations of the given schedule.

        Args:
            schedule: The original schedule to permute
            max_permutations: If provided, stop after generating this many permutations

        Returns:
            A list of Schedules that are conflict-equivalent to the input schedule
        """
        if max_permutations is not None and max_permutations <= 0:
            return []

        ops = schedule.operations
        n = len(ops)

        # Build conflict graph
        graph = schedule.build_conflict_graph()
        indegree = graph.get_in_degree()

        results: list[Schedule] = []

        # Backtracking to enumerate all topological sorts of the partial order
        def backtrack(path: list[int], indeg: list[int], available: list[int]):
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
                for neigh in graph.adjacency[idx]:
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
    
    @classmethod
    def generate_random_conflict_equivalent_permutations(
        cls,
        schedule: Schedule,
        count: int = 10,
        max_attempts: Optional[int] = None
    ) -> list[Schedule]:
        """
        Generate random conflict-equivalent permutations efficiently.
        
        Instead of enumerating all permutations (which can be exponential),
        this method randomly samples topological sorts of the conflict graph.
        This is much faster when there are many possible permutations.
        
        Args:
            schedule: The original schedule to permute
            count: Number of random permutations to generate
            max_attempts: Maximum attempts to find unique permutations (default: count * 100)
                         If None, will keep trying until count unique permutations are found
        
        Returns:
            A list of up to 'count' unique random conflict-equivalent schedules
        """

        if count <= 0:
            return []
        
        ops = schedule.operations
        n = len(ops)
        
        if n == 0:
            return []

        # Build conflict graph
        graph = schedule.build_conflict_graph()
        base_indegree = graph.get_in_degree()
        
        results: list[Schedule] = []
        seen_permutations: set[tuple[int, ...]] = set()
        
        if max_attempts is None:
            max_attempts = count * 100
        
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
                for neighbor in graph.adjacency[idx]:
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