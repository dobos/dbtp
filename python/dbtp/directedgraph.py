from typing import Iterable, Optional, Any, List, Protocol

from .graph import Graph, Vertex, Edge
import math

class CyclicGraphError(Exception):
    """Exception raised when a cycle is detected in the directed graph."""
    pass

class DirectedGraph(Graph):
    """
    Directed graph with adjacency stored as a dict of lists.
    """

    def __init__(
        self,
        vertices: Optional[Iterable[Vertex]] = None,
        edges: Optional[Iterable[Edge]] = None
    ):

        super().__init__()

        self.vertices: dict[int, Vertex] = {}
        self.edges: dict[tuple[int, int], Edge] = {}
        self.adjacency: dict[int, List[int]] = {}
        
        if vertices:
            for v in vertices:
                self.add_vertex(v)

        if edges:
            for e in edges:    
                self.add_edge(e)

    def __str__(self) -> str:
        # Generate a list of the edges in string format
        edges = ""
        for (source, target), e in self.edges.items():
            if e.label is not None:
                edges += f"{self.vertices[source]} -[{e.label}]-> {self.vertices[target]}\n"
            else:
                edges += f"{self.vertices[source]} -> {self.vertices[target]}\n"
        return edges
    
    def latex(
        self,
        radius: float = 1.5
    ) -> str:
        
        """Generate LaTeX representation of the directed graph using TikZ.
        
        Args:
            radius: Radius of the circle on which nodes are arranged (default: 3.0)
        """
        latex_str = "\\begin{tikzpicture}[->,>=Stealth,shorten >=1pt,auto,node distance=3cm, thick,main node/.style={circle,draw,font=\\sffamily\\Large\\bfseries}]\n"
        
        # Add vertices arranged on a circle
        num_vertices = len(self.vertices)
        for i, v in enumerate(sorted(self.vertices.values(), key=lambda v: v.id)):
            angle = 360 * i / num_vertices
            x = radius * math.cos(math.radians(angle))
            y = radius * math.sin(math.radians(angle))
            latex_str += f"\\node[main node] ({v.id}) at ({x:.2f},{y:.2f}) {{$ {v} $}};\n"
        
        # Add edges
        for (source, target), e in self.edges.items():
            label_str = f"[{e.label}]" if e.label is not None else ""
            latex_str += f"\\path ({source}) edge {label_str} ({target});\n"
        
        latex_str += "\\end{tikzpicture}\n"
        return latex_str

    def add_vertex(self, v: Vertex):
        if v.id not in self.vertices:
            self.vertices[v.id] = v
            self.adjacency[v.id] = []
        else:
            raise ValueError(f"Vertex with id {v.id} already exists")
        
    def has_vertex(self, v: Vertex) -> bool:
        """Check if a vertex exists in the graph."""
        return v.id in self.vertices

    def add_edge(self, e: Edge):
        # Ensure both vertices exist in adjacency
        if e.source not in self.vertices:
            raise ValueError(f"Source vertex {e.source} not in graph")

        if e.target not in self.vertices:
            raise ValueError(f"Target vertex {e.target} not in graph")

        if (e.source, e.target) not in self.edges:
            self.edges[(e.source, e.target)] = e
            self.adjacency[e.source].append(e.target)
        else:
            raise ValueError(f"Edge from {e.source} to {e.target} already exists")
        
    def has_edge(self, e: Edge) -> bool:
        """Check if an edge exists from source to target."""
        return (e.source, e.target) in self.edges
    
    def remove_edge(self, e: Edge):
        """Remove an edge from the graph."""
        if (e.source, e.target) in self.edges:
            del self.edges[(e.source, e.target)]
            self.adjacency[e.source].remove(e.target)
        else:
            raise ValueError(f"Edge from {e.source} to {e.target} does not exist")

    def edge_count(self):
        """Return total number of directed edges (counts duplicates)."""
        return len(self.edges)

    def vertex_count(self):
        """Return number of vertices."""
        return len(self.vertices)
    
    def get_out_degree(self):
        """Compute out-degree for each vertex"""
        out_degree = {v: 0 for v in self.adjacency.keys()}
        for source in self.adjacency.keys():
            out_degree[source] = len(self.adjacency[source])
        return out_degree
    
    def get_in_degree(self):
        """Compute in-degree for each vertex"""
        in_degree = {v: 0 for v in self.adjacency.keys()}
        for source in self.adjacency.keys():
            for target in self.adjacency[source]:
                in_degree[target] += 1
        return in_degree
    
    def topological_sort(self):
        """Perform topological sort to get a valid ordering of transactions"""
        
        # Compute in-degree for each vertex
        in_degree = self.get_in_degree()
        
        # Kahn's algorithm for topological sort
        vertices = self.vertices.keys()
        queue = [v for v in vertices if in_degree[v] == 0]
        topo_order = []
        
        while queue:
            # Sort for deterministic output
            queue.sort()
            current = queue.pop(0)
            topo_order.append(current)
            
            # Reduce in-degree for neighbors
            for target in self.adjacency[current]:
                in_degree[target] -= 1
                if in_degree[target] == 0:
                    queue.append(target)
        
        # Check if graph has a cycle
        if len(topo_order) != len(vertices):
            raise CyclicGraphError("Graph contains a cycle")
        
        return topo_order
