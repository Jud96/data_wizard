from  .transformer_process import TransformerProcess
import pandas as pd

class JoinProcess(TransformerProcess):
    def __init__(self, data1, data2, left_on= None, right_on=None, how='inner' ,on=None):
        self.data1 = data1
        self.data2 = data2
        self.how = how
        if on is not None:
            self.on = on
        else:
            self.on = [left_on, right_on]
        if left_on is None and right_on is None:
            self.left_on = on
            self.right_on = on

    def process(self):
        return pd.merge(self.data1, self.data2, on=self.on, how=self.how)