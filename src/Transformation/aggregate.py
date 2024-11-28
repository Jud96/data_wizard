from .transformer_process import TransformerProcess

class AggregateProcess(TransformerProcess):
    def __init__(self, data, group_by_columns,aggregation_columns):
        self.data = data
        self.group_by_columns = group_by_columns
        self.aggregation_columns = aggregation_columns

    def process(self):
        return self.data.groupby(self.group_by_columns).agg(self.aggregation_columns)