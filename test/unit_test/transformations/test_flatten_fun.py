from src.Transformation.flatten import FlattenProcess
import pandas as pd
import unittest

class TestFlattenProcess(unittest.TestCase):
    def test_process(self):
        data = pd.DataFrame({
            'customer_id': [1, 2],
            'name': ['Alice', 'Bob'],
            'orders': [[1, 2], [3, 4]]
        })

        flatten_process = FlattenProcess(data, ['orders'])
        print(flatten_process.process())
        self.assertEqual(len(flatten_process.process()), 4)
        self.assertEqual(flatten_process.process().to_dict(), {
            'customer_id': {0: 1, 0: 1, 1: 2, 1: 2},
            'name': {0: 'Alice', 0: 'Alice', 1: 'Bob', 1: 'Bob'},
            'orders': {0: 1, 0: 2, 1: 3, 1: 4}
        })