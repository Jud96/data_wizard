from src.Transformation.conditional_split import ConditionalSplitProcess
import unittest
import pandas as pd


class TestConditionalSplitProcess(unittest.TestCase):

    def test_process(self):
        data = pd.DataFrame({'order_id': [1, 2, 3, 4],
                             'product': ['A', 'B', 'A', 'B'],
                             'quantity': [10, 20, 30, 40],
                             'price': [100, 200, 300, 400]
                             })
        x, y = ConditionalSplitProcess(
            data=data, condition=data['product'] == 'A').process()
        self.assertEqual(len(x), 2)
        self.assertEqual(len(y), 2)
        self.assertEqual(x.to_dict(), {'order_id': {0: 1, 2: 3},
                                       'product': {0: 'A', 2: 'A'},
                                       'quantity': {0: 10, 2: 30},
                                       'price': {0: 100, 2: 300}})
        self.assertEqual(y.to_dict(), {'order_id': {1: 2, 3: 4},
                                        'product': {1: 'B', 3: 'B'},
                                        'quantity': {1: 20, 3: 40},
                                        'price': {1: 200, 3: 400}})
        
if __name__ == '__main__':
    unittest.main()