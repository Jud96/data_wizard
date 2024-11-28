from .transformer_process import TransformerProcess

class ConditionalSplitProcess(TransformerProcess):
    def __init__(self, data, condition):
        self.data = data
        self.condition = condition

    def process(self):
        output_true = self.data[self.condition]
        output_false = self.data[~self.condition]
        return output_true, output_false