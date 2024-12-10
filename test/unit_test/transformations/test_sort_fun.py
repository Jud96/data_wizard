from src.Transformation.sort import SortProcess
import unittest
import pandas as pd

class TestSortProcess(unittest.TestCase):

    def test_process(self):
        data = pd.DataFrame({'A': [2, 1], 'B': [4, 3]})
        sort = SortProcess(data, columns=['A'], ascending=True)
        result = sort.process()
        print(result)
        expected = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        print(expected)
        self.assertTrue(result.reset_index(drop=True).equals(expected.reset_index(drop=True)))