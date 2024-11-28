from .transformer_process import TransformerProcess

class TransformerDropDuplicates(TransformerProcess):
    def process(self):
        data = self.data.drop_duplicates()
        return data