from extract_service import ExtractService
from urllib.request import urlopen
from bs4 import BeautifulSoup
from pandas import DataFrame

class ExtractServiceFromWebScraper(ExtractService):
    def __init__(self, base_url):
        self.url = base_url

    def extract(self):
        page = urlopen(self.base_url)
        html = page.read().decode("utf-8")
        soup = BeautifulSoup(html, "html.parser")
        page_span = soup.find("span")
        number_of_pages = int(page_span.string.split()[-1])

        data = []
        for i in range(1, number_of_pages + 1):
            url = f"{self.base_url}?page={i}"
            page = urlopen(url)
            html = page.read().decode("utf-8")
            soup = BeautifulSoup(html, "html.parser")
            table = soup.find("table")
            rows = table.find_all("tr")
            for row in rows[1:]:
                cols = row.find_all("td")
                data.append([element.text for element in cols])
        df = DataFrame(data, columns=["id", "name", "level"])
        df['source'] = 'web'