from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Dict, Set, Optional, Iterable

class OperationType(Enum):
    READ = auto()
    WRITE = auto()
    LOCK = auto()
    XLOCK = auto()
    SLOCK = auto()
    UNLOCK = auto()
    COMMIT = auto()
    ROLLBACK = auto()

@dataclass
class Operation:
    tx: int
    op: OperationType
    item: str = None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Operation):
            return NotImplementedError()
        return self.tx == other.tx and self.op == other.op and self.item == other.item

    def __repr__(self) -> str:
        return f"Operation(tx={self.tx}, op={self.op}, item={self.item})"

    def __str__(self):
        if self.op in {
            OperationType.READ,
            OperationType.WRITE,
            OperationType.LOCK,
            OperationType.UNLOCK
        }:
            return f"{self.op.name[0]}_{self.tx}({self.item})"
        elif self.op in {
            OperationType.SLOCK,
            OperationType.XLOCK,
        }:
            return f"{self.op.name[:2]}_{self.tx}({self.item})"
        elif self.op == OperationType.COMMIT:
            return f"COMMIT_{self.tx}"
        elif self.op == OperationType.ROLLBACK:
            return f"ROLLBACK_{self.tx}"
        else:
            return f"UNKNOWN_OP_{self.tx}"
        
    def latex(self) -> str:
        if self.op in {
            OperationType.READ,
            OperationType.WRITE,
            OperationType.LOCK,
            OperationType.UNLOCK
        }:
            return f"{self.op.name[0].lower()}_{{{self.tx}}}({self.item})"
        elif self.op in {
            OperationType.SLOCK,
            OperationType.XLOCK,
        }:
            return f"{self.op.name[:2].lower()}_{{{self.tx}}}({self.item})"
        elif self.op == OperationType.COMMIT:
            return f"\\text{{COMMIT}}_{{{self.tx}}}"
        elif self.op == OperationType.ROLLBACK:
            return f"\\text{{ROLLBACK}}_{{{self.tx}}}"
        else:
            return f"\\text{{UNKNOWN\_OP}}_{{{self.tx}}}"
        
    @staticmethod
    def parse(value: str) -> 'Operation':
        """Parse a string representation of an operation back to an Operation object.
        
        Examples: R_1(A), W_2(B), COMMIT_3, ROLLBACK_4, L_1(X), SL_2(Y), XL_3(Z)
        """
        value = value.strip()
        
        # Handle COMMIT
        if value.startswith("COMMIT_"):
            tx = int(value.split("_")[1])
            return Operation(tx, OperationType.COMMIT)
        
        # Handle ROLLBACK
        if value.startswith("ROLLBACK_"):
            tx = int(value.split("_")[1])
            return Operation(tx, OperationType.ROLLBACK)
        
        # Handle operations with items: R_1(A), W_2(B), etc.
        if "(" in value and value.endswith(")"):
            op_part, item_part = value.split("(")
            item = item_part[:-1]  # Remove closing parenthesis
            
            parts = op_part.split("_")
            op_name = parts[0]
            tx = int(parts[1])
            
            op_map = {
                "R": OperationType.READ,
                "W": OperationType.WRITE,
                "L": OperationType.LOCK,
                "SL": OperationType.SLOCK,
                "XL": OperationType.XLOCK,
                "U": OperationType.UNLOCK
            }
            
            if op_name in op_map:
                return Operation(tx, op_map[op_name], item)
        
        raise ValueError(f"Cannot parse operation: {value}")
        
    def is_in_conflict_with(self, other: 'Operation') -> bool:
        if self.item != other.item:
            return False
        if self.tx == other.tx:
            return False
        if self.op == OperationType.READ and other.op == OperationType.READ:
            return False
        return True