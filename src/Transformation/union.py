from  .transformer_process import TransformerProcess
import pandas as pd 

class UnionProcess(TransformerProcess):

    def __init__(self, data1, data2,by_row=True):
        self.data1 = data1
        self.data2 = data2
        self.by_row = by_row

    def process(self):
        # check that the columns are the same
        if self.by_row:
            return pd.concat([self.data1, self.data2], axis=0)
        else:
            return pd.concat([self.data1, self.data2], axis=1)
