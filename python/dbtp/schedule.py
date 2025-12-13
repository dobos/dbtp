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