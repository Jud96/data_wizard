from src.Transformation.aggregate import AggregateProcess
import unittest
import pandas as pd

class TestAggregateProcess(unittest.TestCase):

    def test_process(self):
        sales = pd.DataFrame({
            'order_id': [1, 2, 3, 4, 5, 6],
            'date': ['2021-01-01', '2021-01-01', '2021-01-02', '2021-01-02', '2021-01-03', '2021-01-03'],
            'customer_id': [1, 2, 1, 2, 1, 2],
            'product': ['A', 'B', 'A', 'B', 'A', 'B'],
            'quantity': [10, 20, 30, 40, 50, 60],
            'price': [100, 200, 300, 400, 500, 600]
        })

        aggregate_process = AggregateProcess(sales, group_by_columns=['customer_id'],
                                             aggregation_columns={'quantity': 'sum', 'price': 'sum'})
        print(aggregate_process.process().to_dict())
        self.assertEqual(aggregate_process.process().to_dict(), 
                         {'quantity': {1: 90, 2: 120}, 'price': {1: 900, 2: 1200}})

        

if __name__ == '__main__':
    unittest.main()