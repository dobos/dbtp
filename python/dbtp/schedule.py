from typing import Iterable, Optional, Any, List, Protocol

from .constants import Constants
from .operation import OperationType, Operation
from .directedgraph import DirectedGraph, Vertex, Edge, CyclicGraphError

class Schedule():
    def __init__(
        self,
        id: int = None,
        operations: Optional[Iterable[Operation]] = None
    ):
        self.id: int = id
        self.operations: List[Operation] = list(operations) if operations else []

    def __repr__(self) -> str:
        return f"Schedule(id={self.id}, operations={list.__repr__(self.operations)})"
    
    def __str__(self) -> str:
        return f"S_{self.id} : {', '.join(str(op) for op in self.operations)}"
    
    def latex(self) -> str:
        return f"S_{{{self.id}}} : {', '.join(op.latex() for op in self.operations)}"
    
    @staticmethod
    def parse(value: str) -> 'Schedule':
        """Parse a string representation of a schedule back to a Schedule object.
        
        Example: S_1 : R_1(A), W_1(B), COMMIT_1
        """
        value = value.strip()
        
        if not value.startswith("S_"):
            raise ValueError("Invalid schedule format")
        
        id_part, ops_part = value.split(":", 1)
        schedule_id = int(id_part.split("_")[1].strip())
        
        operations = []
        for op_str in ops_part.split(","):
            op_str = op_str.strip()
            if op_str:
                operation = Operation.parse(op_str)
                operations.append(operation)
        
        return Schedule(id=schedule_id, operations=operations)

    def build_conflict_graph(self) -> DirectedGraph:
        """
        Build a conflict graph for the given schedule.
        Returns a DirectedGraph whose vertices are operation indices and an indegree list.
        """
        ops = self.operations
        n = len(ops)

        # Create a DirectedGraph instance without calling its constructor to avoid signature issues
        vertices = [Vertex(id=i, label=ops[i]) for i in range(n)]
        graph = DirectedGraph(vertices=vertices)

        for i in range(n):
            for j in range(i + 1, n):
                if ops[i].is_in_conflict_with(ops[j]):
                    graph.add_edge(Edge(source=i, target=j))

        return graph
    
    def is_conflict_equivalent_with(self, other: 'Schedule') -> bool:
        """
        Check if this schedule is conflict-equivalent with another schedule.
        """
        if len(self.operations) != len(other.operations):
            return False
        
        g1 = self.build_conflict_graph()
        g2 = other.build_conflict_graph()
        
        return self.are_conflict_graphs_isomorphic(g1, g2)
    
    def are_conflict_graphs_isomorphic(self, this: DirectedGraph, other: DirectedGraph) -> bool:
        """Check if this directed graph is isomorphic to another directed graph."""
        
        # Make sure both graphs have the same set of vertex labels
        this_vertices = { str(v.label) for v in this.vertices.values() }
        other_vertices = { str(v.label) for v in other.vertices.values() }
        if this_vertices != other_vertices:
            return False
        
        # Make sure both graphs have the same set of edges
        this_edges = { (str(this.vertices[source].label),
                        str(this.vertices[target].label))
                       for (source, target), e in this.edges.items() }
        other_edges = { (str(other.vertices[source].label),
                         str(other.vertices[target].label))
                       for (source, target), e in other.edges.items() }

        if this_edges != other_edges:
            return False
        
        return True
    
    def build_precedence_graph(self) -> DirectedGraph:
        """
        Build a precedence graph for the given schedule.
        Returns a DirectedGraph whose vertices are transaction IDs.
        """
        ops = self.operations
        transactions = {}
        
        # Create vertices for each transaction
        for op in ops:
            if op.tx not in transactions:
                transactions[op.tx] = Vertex(id=op.tx, label=op.tx)
        
        graph = DirectedGraph(vertices=list(transactions.values()))
        
        # Add edges based on operation order
        for i in range(len(ops)):
            for j in range(i + 1, len(ops)):
                if ops[i].tx != ops[j].tx and ops[i].is_in_conflict_with(ops[j]):
                    edge = Edge(source=ops[i].tx, target=ops[j].tx)
                    if not graph.has_edge(edge):
                        graph.add_edge(edge)
        
        return graph
    
    def is_conflict_serializable(self) -> bool:
        """
        Check if the schedule is conflict-serializable by verifying if its conflict graph is acyclic.
        """
        graph = self.build_precedence_graph()
        
        try:
            graph.topological_sort()
            return True
        except CyclicGraphError:
            return False
        
    def serialize(self) -> 'Schedule':
        """
        Return a serial schedule equivalent to this schedule if it is conflict-serializable.
        Raises an error if the schedule is not conflict-serializable.
        """
        if not self.is_conflict_serializable():
            raise ValueError("Schedule is not conflict-serializable")
        
        precedence_graph = self.build_precedence_graph()
        sorted_tx_ids = precedence_graph.topological_sort()
        
        serial_operations = []
        for tx_id in sorted_tx_ids:
            for op in self.operations:
                if op.tx == tx_id:
                    serial_operations.append(op)
        
        return Schedule(id=self.id, operations=serial_operations)
    
    def build_wait_for_graph(self) -> DirectedGraph:
        """
        Build a wait-for graph for the given schedule.
        Returns a DirectedGraph whose vertices are transaction IDs.
        Uses XLOCK, SLOCK, and UNLOCK operations to track lock conflicts.
        """
        ops = self.operations
        transactions = {}
        
        # Create vertices for each transaction
        for op in ops:
            if op.tx not in transactions:
                transactions[op.tx] = Vertex(id=op.tx, label=op.tx)
        
        graph = DirectedGraph(vertices=list(transactions.values()))
        
        lock_table = {}
        
        for op in ops:
            item = op.item
            tx_id = op.tx
            
            if item not in lock_table:
                lock_table[item] = {'shared_locks': set(), 'exclusive_lock': None}
            
            locks = lock_table[item]
            
            if op.op == OperationType.SLOCK:
                # S-lock waits if there's an X-lock by another transaction
                if locks['exclusive_lock'] is not None and locks['exclusive_lock'] != tx_id:
                    graph.add_edge(Edge(source=tx_id, target=locks['exclusive_lock']))
                locks['shared_locks'].add(tx_id)
            
            elif op.op == OperationType.XLOCK:
                # X-lock waits for all other S-locks
                for holder in locks['shared_locks']:
                    if holder != tx_id:
                        graph.add_edge(Edge(source=tx_id, target=holder))
                # X-lock waits if there's an X-lock by another transaction
                if locks['exclusive_lock'] is not None and locks['exclusive_lock'] != tx_id:
                    graph.add_edge(Edge(source=tx_id, target=locks['exclusive_lock']))
                locks['exclusive_lock'] = tx_id
                locks['shared_locks'].discard(tx_id)
            
            elif op.op == OperationType.UNLOCK:
                # Release locks
                if locks['exclusive_lock'] == tx_id:
                    locks['exclusive_lock'] = None
                locks['shared_locks'].discard(tx_id)
        
        return graph
    
    def has_deadlock(self) -> bool:
        """Test if the schedule has a deadlock by checking for cycles in the wait-for graph."""
        graph = self.build_wait_for_graph()
        
        try:
            graph.topological_sort()
            return False
        except CyclicGraphError:
            return True
        
    def is_legal(self) -> bool:
        """
        Check if the schedule is legal with respect to locking protocols.
        Ensures that no operation is performed on an item without holding the appropriate lock.
        """
        lock_table = {}
        
        for op in self.operations:
            item = op.item
            tx_id = op.tx
            
            if item not in lock_table:
                lock_table[item] = {'shared_locks': set(), 'exclusive_lock': None}
            
            locks = lock_table[item]
            
            if op.op == OperationType.SLOCK:
                locks['shared_locks'].add(tx_id)
            
            elif op.op == OperationType.XLOCK:
                locks['exclusive_lock'] = tx_id
                locks['shared_locks'].discard(tx_id)
            
            elif op.op == OperationType.UNLOCK:
                if locks['exclusive_lock'] == tx_id:
                    locks['exclusive_lock'] = None
                locks['shared_locks'].discard(tx_id)
            
            elif op.op in {OperationType.READ, OperationType.WRITE}:
                # Check if the transaction holds the necessary lock
                if op.op == OperationType.READ:
                    if (locks['exclusive_lock'] != tx_id and
                        tx_id not in locks['shared_locks']):
                        return False
                elif op.op == OperationType.WRITE:
                    if locks['exclusive_lock'] != tx_id:
                        return False
        
        return True
    
    def is_two_phase_locked(self) -> bool:
        """
        Check if the schedule follows the two-phase locking protocol.
        Ensures that each transaction has a growing phase (acquiring locks)
        followed by a shrinking phase (releasing locks) without interleaving.
        """
        tx_phases = {}
        
        for op in self.operations:
            tx_id = op.tx
            
            if tx_id not in tx_phases:
                tx_phases[tx_id] = 'growing'
            
            phase = tx_phases[tx_id]
            
            if op.op in {OperationType.SLOCK, OperationType.XLOCK}:
                if phase == 'shrinking':
                    return False  # Cannot acquire locks in shrinking phase
            
            elif op.op == OperationType.UNLOCK:
                tx_phases[tx_id] = 'shrinking'  # Transition to shrinking phase
        
        return True
    
    def add_locks(
        self,
        use_shared_locks: bool = False
    ) -> "Schedule":
        """
        Add lock and unlock operations to the given schedule.

        For each READ or WRITE operation, a LOCK operation is added before it,
        and an UNLOCK operation is added after the last access to that item by the transaction.
        Does NOT follow two-phase locking - locks are released as soon as possible.

        Args:
            use_shared_locks: Whether to use SLOCK/XLOCK or just LOCK
        Returns:
            A new Schedule with LOCK and UNLOCK operations added
        """

        # Track which items each transaction has locked and the lock type
        locked_items = {tx: {} for tx in set(op.tx for op in self.operations)}
        new_operations = []
        
        for i, op in enumerate(self.operations):
            if op.op in {OperationType.READ, OperationType.WRITE}:
                # Determine required lock type
                if use_shared_locks:
                    required_lock = OperationType.SLOCK if op.op == OperationType.READ else OperationType.XLOCK
                else:
                    required_lock = OperationType.LOCK
                
                # Check current lock status
                current_lock = locked_items[op.tx].get(op.item)
                
                if current_lock is None:
                    # No lock held, acquire the required lock
                    new_operations.append(Operation(tx=op.tx, op=required_lock, item=op.item))
                    locked_items[op.tx][op.item] = required_lock
                        
                elif use_shared_locks and current_lock == OperationType.SLOCK and required_lock == OperationType.XLOCK:
                    # Upgrade from SLOCK to XLOCK
                    new_operations.append(Operation(tx=op.tx, op=OperationType.XLOCK, item=op.item))
                    locked_items[op.tx][op.item] = OperationType.XLOCK
                
                # Add the original operation
                new_operations.append(op)
                
                # Check if this is the last access to this item by this transaction
                last_use = True
                for j in range(i + 1, len(self.operations)):
                    future_op = self.operations[j]
                    if future_op.tx == op.tx and future_op.item == op.item and future_op.op in {OperationType.READ, OperationType.WRITE}:
                        last_use = False
                        break
                
                # Unlock immediately after last access
                if last_use:
                    new_operations.append(Operation(tx=op.tx, op=OperationType.UNLOCK, item=op.item))
                    locked_items[op.tx].pop(op.item, None)
            else:
                # Non-read/write operations are added as-is
                new_operations.append(op)
                
                # If transaction ends (COMMIT/ABORT), release any remaining locks
                if op.op in {OperationType.COMMIT, OperationType.ABORT}:
                    for item in list(locked_items[op.tx].keys()):
                        new_operations.append(Operation(tx=op.tx, op=OperationType.UNLOCK, item=item))
                    locked_items[op.tx].clear()
        
        # Release any remaining locks at the end of the schedule
        for tx in locked_items:
            for item in list(locked_items[tx].keys()):
                new_operations.append(Operation(tx=tx, op=OperationType.UNLOCK, item=item))
        
        return Schedule(id=self.id, operations=new_operations)
    
    def add_locks_two_phase(
        self,
        use_shared_locks: bool = False
    ) -> "Schedule":
        pass
