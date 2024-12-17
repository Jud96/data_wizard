from src.Transformation.derived_column import DerivedColumnProcess
import unittest
import pandas as pd

class TestDerivedColumn(unittest.TestCase):
    
        def test_process(self):
            data = pd.DataFrame({'order_id': [1, 2, 3, 4],
                                'product': ['A', 'B', 'A', 'B'],
                                'quantity': [10, 20, 30, 40],
                                'price': [100, 200, 300, 400]
                                })
            x = DerivedColumnProcess(data=data, new_column_name='total',
                                     expression=data['quantity']*data['price']).process()
            self.assertEqual(x.to_dict(), {'order_id': {0: 1, 1: 2, 2: 3, 3: 4},
                                        'product': {0: 'A', 1: 'B', 2: 'A', 3: 'B'},
                                        'quantity': {0: 10, 1: 20, 2: 30, 3: 40},
                                        'price': {0: 100, 1: 200, 2: 300, 3: 400},
                                        'total': {0: 1000, 1: 4000, 2: 9000, 3: 16000}})