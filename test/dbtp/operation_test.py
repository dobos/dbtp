import unittest

from dbtp.operation import Operation, OperationType

class OperationTest(unittest.TestCase):
    def test_str_prefixed_ops(self):
        cases = [
            (OperationType.READ, "x", 1, "R_1(x)"),
            (OperationType.WRITE, "y", 2, "W_2(y)"),
            (OperationType.LOCK, "z", 3, "L_3(z)"),
            (OperationType.SLOCK, "s", 4, "S_4(s)"),
            (OperationType.XLOCK, "t", 5, "X_5(t)"),
        ]
        for op_type, item, tx, expected in cases:
            with self.subTest(op_type=op_type, item=item, tx=tx):
                self.assertEqual(str(Operation(tx=tx, op=op_type, item=item)), expected)

    def test_str_commit_and_rollback(self):
        self.assertEqual(str(Operation(tx=10, op=OperationType.COMMIT)), "COMMIT_10")
        self.assertEqual(str(Operation(tx=11, op=OperationType.ROLLBACK)), "ROLLBACK_11")

    def test_str_unknown_op_unlock(self):
        self.assertEqual(str(Operation(tx=7, op=OperationType.UNLOCK)), "7:UNKNOWN_OP")

    def test_str_item_none_shows_None(self):
        self.assertEqual(str(Operation(tx=9, op=OperationType.READ, item=None)), "R_9(None)")

    def test_parse(self):
        cases = [
            ("R_1(A)", Operation(tx=1, op=OperationType.READ, item="A")),
            ("W_2(B)", Operation(tx=2, op=OperationType.WRITE, item="B")),
            ("L_3(C)", Operation(tx=3, op=OperationType.LOCK, item="C")),
            ("SL_4(D)", Operation(tx=4, op=OperationType.SLOCK, item="D")),
            ("XL_5(E)", Operation(tx=5, op=OperationType.XLOCK, item="E")),
            ("COMMIT_6", Operation(tx=6, op=OperationType.COMMIT)),
            ("ROLLBACK_7", Operation(tx=7, op=OperationType.ROLLBACK)),
        ]
        for input_str, expected_op in cases:
            with self.subTest(input_str=input_str):
                parsed_op = Operation.parse(input_str)
                self.assertEqual(parsed_op.tx, expected_op.tx)
                self.assertEqual(parsed_op.op, expected_op.op)
                self.assertEqual(parsed_op.item, expected_op.item)

if __name__ == "__main__":
    unittest.main()