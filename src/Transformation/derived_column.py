from  .transformer_process import TransformerProcess
class DerivedColumnProcess(TransformerProcess):
    def __init__(self, data, new_column_name, expression):
        self.data = data
        self.new_column_name = new_column_name
        self.expression = expression


    def process(self):
        self.data[self.new_column_name] = self.data.eval(self.expression)
        return self.data