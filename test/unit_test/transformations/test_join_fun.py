from src.Transformation.join import JoinProcess
import pandas as pd
import unittest

class TestJoinProcess(unittest.TestCase):

    def test_process(self):
        data1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        data2 = pd.DataFrame({'A': [1, 2], 'C': [5, 6]})
        join = JoinProcess(data1, data2, on='A')
        result = join.process()
        expected = pd.DataFrame({'A': [1, 2], 'B': [3, 4], 'C': [5, 6]})
        self.assertTrue(expected.equals(result))