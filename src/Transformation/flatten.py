from  .transformer_process import TransformerProcess
import pandas as pd

class FlattenProcess(TransformerProcess):
    def __init__(self, data, columns):
        self.data = data
        self.columns = columns


    def process(self):
        # Flatten the data
        return self.data.explode(self.columns)