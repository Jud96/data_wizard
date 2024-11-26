from extract_service import ExtractService
from abc import abstractmethod

class ExtractService:
    def __init__(self, data):
        self.data = data

    @abstractmethod
    def extract(self):
        pass