from  .transformer_process import TransformerProcess

class FilterProcess(TransformerProcess):
    def __init__(self, data, condition):
        self.data = data
        self.condition = condition

    def process(self):
        return self.data[self.condition]