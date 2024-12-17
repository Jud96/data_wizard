from .transformer_process import TransformerProcess


class PivotProcess(TransformerProcess):
    def __init__(self, data, index, columns, values,aggfunc='sum'):
        self.data = data
        self.index = index
        self.columns = columns
        self.values = values
        self.aggfunc = aggfunc

    def process(self):
        return self.data.pivot_table(index=self.index,
                                     columns=self.columns,
                                     values=self.values,
                                     aggfunc=self.aggfunc)
