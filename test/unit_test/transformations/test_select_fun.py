from src.Transformation.select import SelectProcess
import pandas as pd
import unittest

class TestSelectProcess(unittest.TestCase):

    def test_process(self):
        data = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        select = SelectProcess(data, columns=['A'])
        result = select.process()
        expected = pd.DataFrame({'A': [1, 2]})
        self.assertTrue(expected.equals(result))