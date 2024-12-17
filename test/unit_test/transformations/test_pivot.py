from src.Transformation.pivot import PivotProcess
import unittest
import pandas as pd


class TestPivot(unittest.TestCase):

    def test_process(self):
        data = pd.DataFrame({'order_id': [1, 1, 1, 1],
                            'product': ['A', 'B', 'A', 'B'],
                             'quantity': [10, 20, 30, 40],
                             'price': [100, 200, 300, 400]
                             })
        x = PivotProcess(data=data, index='order_id', columns='product',
                            values='quantity').process()
        print(x)
        self.assertEqual(x.to_dict(), {'A': {1: 40},
                                       'B': {1: 60}})
