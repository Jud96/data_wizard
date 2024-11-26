from .extract_service import ExtractService
import pandas as pd
import xml.etree.ElementTree as ET
class ExtractServiceFromXML(ExtractService):
    def __init__(self, file_path):
        self.file = file_path
    def extract(self):
        tree = ET.parse(self.file)
        root = tree.getroot()
        data = []
        for item in root:
            item_data = {}
            for child in item:
                item_data[child.tag] = child.text
            data.append(item_data)
        df = pd.DataFrame(data)
        df['source'] = 'xml'
        return df