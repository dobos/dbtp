import random
from typing import Optional
from .constants import Constants

from .schedule_generator import ScheduleGenerator
from .directed_graph import DirectedGraph, CyclicGraphError, Vertex, Edge
from .operation import Operation, OperationType
from .schedule import Schedule

class WaitforScheduleGenerator(ScheduleGenerator):
    pass


    def generate_random_wait_for_graph(
        self,
        transaction_count: int = 4,
        edge_count: int = 4,
        acyclic: bool = True,
        cyclic: bool = False,
    ) -> DirectedGraph:

        """
        Generate a random wait-for graph with the specified number of transactions.

        A cycle in a wait-for graph indicates deadlock. Use acyclic=True to guarantee
        no deadlock and cyclic=True to guarantee at least one deadlock cycle.

        Args:
            transaction_count: Number of transactions (vertices) in the graph
            edge_count: Number of edges to add to the graph
            acyclic: If True, keep the graph acyclic
            cyclic: If True, force the graph to contain at least one cycle
        Returns:
            A DirectedGraph representing the wait-for graph
        """

        return self._generate_random_directed_graph(
            node_count=transaction_count,
            edge_count=edge_count,
            acyclic=acyclic,
            cyclic=cyclic,
            failure_message="Failed to generate requested wait-for graph within max attempts",
        )

    def generate_schedule_from_wait_for_graph(
        self,
        graph: DirectedGraph
    ) -> Schedule:
        
        """
        Generate a randomized schedule with SLOCK/XLOCK and strict 2PL that realizes
        the given wait-for graph during execution.

        For each wait-for edge (i -> j), transaction j first locks an item, then
        transaction i requests an XLOCK on the same item. This introduces the wait edge.

        Strict 2PL is enforced by releasing all locks only at transaction end.

        Args:
            graph: Wait-for graph where vertices are transaction IDs and edges i -> j
                   mean transaction i waits for transaction j.

        Returns:
            A randomized schedule that is legal and strict-2PL.
        """
        operations = []

        # Build per-edge items and randomized holder lock choice.
        edge_defs = []
        item_counter = 0
        for (source, target), edge in graph.edges.items():
            if edge.label is None:
                if item_counter < len(Constants.LETTERS):
                    item_name = f"{Constants.LETTERS[item_counter]}"
                else:
                    item_name = f"X{item_counter}"
            else:
                item_name = edge.label

            # Holder lock can be shared or exclusive; requester uses XLOCK to wait.
            holder_lock = random.choice([OperationType.SLOCK, OperationType.XLOCK])
            edge_defs.append((source, target, item_name, holder_lock))
            item_counter += 1

        # Randomize edge order to keep generated schedules diverse.
        random.shuffle(edge_defs)

        # For each edge e, create two events:
        # A_e: holder acquires lock on item
        # B_e: waiter requests XLOCK on same item (produces wait edge waiter -> holder)
        # Constraint: A_e must happen before B_e.
        event_payload = {}
        successors = {}
        indegree = {}

        for idx, (source, target, item_name, holder_lock) in enumerate(edge_defs):
            a_id = f"A{idx}"
            b_id = f"B{idx}"

            event_payload[a_id] = (target, holder_lock, item_name)
            event_payload[b_id] = (source, OperationType.XLOCK, item_name)

            successors[a_id] = [b_id]
            successors[b_id] = []
            indegree[a_id] = 0
            indegree[b_id] = 1

        # Randomized topological order of events respecting A_e -> B_e constraints.
        event_order = []
        available = [eid for eid, deg in indegree.items() if deg == 0]
        while available:
            chosen = random.choice(available)
            available.remove(chosen)
            event_order.append(chosen)

            for nxt in successors[chosen]:
                indegree[nxt] -= 1
                if indegree[nxt] == 0:
                    available.append(nxt)

        # Keep per-transaction lock ownership to emit strict-2PL unlocks at the end.
        locked_items_by_tx = {tx: [] for tx in graph.vertices}
        seen_items_by_tx = {tx: set() for tx in graph.vertices}
        used_items = {item_name for _, _, item_name, _ in edge_defs}

        for event_id in event_order:
            tx, lock_op, item = event_payload[event_id]

            # Each (tx, item) pair appears once by construction, but guard anyway.
            if item not in seen_items_by_tx[tx]:
                operations.append(Operation(tx=tx, op=lock_op, item=item))
                seen_items_by_tx[tx].add(item)
                locked_items_by_tx[tx].append(item)

            # Add an access operation compatible with the lock to keep schedule meaningful.
            if lock_op == OperationType.SLOCK:
                operations.append(Operation(tx=tx, op=OperationType.READ, item=item))
            else:
                operations.append(Operation(tx=tx, op=OperationType.WRITE, item=item))

        # Ensure all transactions perform at least one lock/access operation.
        idle_txs = [tx for tx in graph.vertices if not seen_items_by_tx[tx]]
        random.shuffle(idle_txs)
        for tx in idle_txs:
            solo_item = f"TX{tx}_SOLO"
            while solo_item in used_items:
                solo_item = f"{solo_item}_X"
            used_items.add(solo_item)

            operations.append(Operation(tx=tx, op=OperationType.XLOCK, item=solo_item))
            operations.append(Operation(tx=tx, op=OperationType.WRITE, item=solo_item))
            seen_items_by_tx[tx].add(solo_item)
            locked_items_by_tx[tx].append(solo_item)

        # Strict 2PL tail: release all held locks at transaction end.
        tx_order = list(graph.vertices.keys())
        random.shuffle(tx_order)

        for tx in tx_order:
            unlock_items = locked_items_by_tx.get(tx, []).copy()
            random.shuffle(unlock_items)
            for item in unlock_items:
                operations.append(Operation(tx=tx, op=OperationType.UNLOCK, item=item))

        return Schedule(id=1, operations=operations)