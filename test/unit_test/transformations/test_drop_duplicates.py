from src.Transformation.drop_duplicates import TransformerDropDuplicates
import pandas as pd
import unittest


class TestDropDuplicates(unittest.TestCase):

    def test_process(self):
        data = pd.DataFrame({'A': [1, 2, 1], 'B': [3, 4, 3]})
        drop_duplicates = TransformerDropDuplicates(data)
        result = drop_duplicates.process()
        expected = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        self.assertTrue(expected.equals(result))
