from src.Transformation.union import UnionProcess
import pandas as pd
import unittest

class TestUnionProcess(unittest.TestCase):

    def test_process_by_row(self):
        data1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        data2 = pd.DataFrame({'A': [1, 2], 'B': [5, 6]})
        union = UnionProcess(data1, data2, by_row=True)
        result = union.process()
        expected_A = [1, 2, 1, 2]
        expected_B = [3, 4, 5, 6]
        self.assertEqual(expected_A, result['A'].tolist())
        self.assertEqual(expected_B, result['B'].tolist())

    def test_process_by_column(self):
        data1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        data2 = pd.DataFrame({'C': [5, 6], 'D': [7, 8]})
        union = UnionProcess(data1, data2, by_row=False)
        result = union.process()
        expected = pd.DataFrame({'A': [1, 2], 'B': [3, 4], 'C': [5, 6], 'D': [7, 8]})
        self.assertTrue(expected.equals(result))