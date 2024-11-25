from abc import ABC, abstractmethod

class LinkedService(ABC):

    @abstractmethod
    def connect(self):
        pass