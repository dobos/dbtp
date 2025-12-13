from dataclasses import dataclass, field

@dataclass(frozen=True)
class Vertex:
    id: int
    label: object = field(default=None)

@dataclass(frozen=True)
class Edge:
    source: int
    target: int
    label: object = field(default=None)

class Graph():
    pass