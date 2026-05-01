import random
from typing import Optional
from .constants import Constants
from .directed_graph import DirectedGraph, CyclicGraphError, Vertex, Edge
from .operation import Operation, OperationType
from .schedule import Schedule


class ScheduleGenerator():
    """
    
    Variables:
    ----------

    max_attempts: Optional[int]
        Maximum attempts to find unique permutations (default: count * 100)
        If None, will keep trying until count unique permutations are found
    """

    def __init__(
        self,
        max_attempts: Optional[int] = None
    ):
        
        self.__max_attempts = None

    def __get_max_attempts(self):
        return self.__max_attempts
    
    def __set_max_attempts(self, value):
        self.__max_attempts = value

    max_attempts = property(__get_max_attempts, __set_max_attempts)

    def _generate_random_directed_graph(
        self,
        node_count: int,
        edge_count: int,
        acyclic: bool,
        cyclic: bool,
        failure_message: str,
    ) -> DirectedGraph:
        
        """Generate a random directed graph with optional acyclic/cyclic constraints."""

        if cyclic and acyclic:
            raise ValueError("Graph cannot be both cyclic and acyclic")

        vertices = [Vertex(id=i, label=i) for i in range(1, node_count + 1)]
        graph = DirectedGraph(vertices=vertices)

        added_edges = 0

        if cyclic:
            # Create a random cycle to guarantee cyclicity.
            cycle_length = min(random.randint(2, node_count), edge_count)
            cycle_vertices = random.sample(range(1, node_count + 1), cycle_length)

            for i in range(cycle_length):
                src = cycle_vertices[i]
                dst = cycle_vertices[(i + 1) % cycle_length]
                graph.add_edge(Edge(source=src, target=dst))

            added_edges = cycle_length

        max_attempts = edge_count * 20 if self.__max_attempts is None else self.__max_attempts
        attempts = 0

        while added_edges < edge_count and attempts < max_attempts:
            attempts += 1
            src = random.randint(1, node_count)
            dst = random.randint(1, node_count)

            if src == dst:
                continue

            edge = Edge(source=src, target=dst)

            if graph.has_edge(edge):
                continue

            graph.add_edge(edge)

            if acyclic:
                try:
                    graph.topological_sort()
                    added_edges += 1
                except CyclicGraphError:
                    graph.remove_edge(edge)
            else:
                added_edges += 1

        if attempts == max_attempts:
            raise RuntimeError(failure_message)

        return graph

    
    