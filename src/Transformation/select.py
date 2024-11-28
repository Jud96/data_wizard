from  .transformer_process import TransformerProcess
         
class SelectProcess(TransformerProcess):
    def __init__(self, data, columns):
        self.data = data
        self.columns = columns

    def process(self):
        return self.data[self.columns]