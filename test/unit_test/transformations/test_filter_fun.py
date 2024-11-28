from src.Transformation.filter import FilterProcess
import pandas as pd
import unittest


class TestFilterProcess(unittest.TestCase):
    def test_process(self):
        data = pd.DataFrame({'order_id': [1, 2, 3, 4],
                             'product': ['A', 'B', 'A', 'B'],
                             'quantity': [10, 20, 30, 40],
                             'price': [100, 200, 300, 400]
                             })

        filter_process = FilterProcess(data, data['product'] == 'A')
        self.assertEqual(len(filter_process.process()), 2)
        self.assertEqual(filter_process.process().to_dict(),
                         {'order_id': {0: 1, 2: 3},
                          'product': {0: 'A', 2: 'A'},
                          'quantity': {0: 10, 2: 30},
                          'price': {0: 100, 2: 300}})
