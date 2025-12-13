from dataclasses import dataclass, field

@dataclass(frozen=True)
class Vertex:
    id: int
    label: object = field(default=None)

    def __str__(self) -> str:
        if self.label is not None:
            return f"{self.label}"
        else:
            return f"V_{self.id}"

@dataclass(frozen=True)
class Edge:
    source: int
    target: int
    label: object = field(default=None)

    def __str__(self) -> str:
        if self.label is not None:
            return f"{self.source} --[{self.label}]--> {self.target}"
        else:
            return f"{self.source} -------> {self.target}"

class Graph():
    pass