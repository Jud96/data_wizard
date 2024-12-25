from src.Datasets.dataset import Dataset

class CopyActivity:
    def __init__(self, source: Dataset, sink : Dataset, mapping: dict):
        self.source = source
        self.sink = sink
        self.mapping = mapping

    def copy(self):
       try:
            data = self.source.extract()
            if self.mapping:
                data.rename(columns=self.mapping, inplace=True)
                data = data[list(self.mapping.values())]
            self.sink.load(data)
       except Exception as e:
            print(f"Error during data loading: {e}")