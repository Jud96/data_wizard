from  .transformer_process import TransformerProcess

    
class SortProcess(TransformerProcess):
    def __init__(self, data, columns, ascending=True):
        self.data = data
        self.columns = columns
        self.ascending = ascending

    def process(self):
        return self.data.sort_values(by=self.columns, ascending=self.ascending)