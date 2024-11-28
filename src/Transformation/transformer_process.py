from abc import ABC, abstractmethod

class TransformerProcess(ABC):
    def __init__(self, data):
        self.data = data

    @abstractmethod
    def process(self):
        pass