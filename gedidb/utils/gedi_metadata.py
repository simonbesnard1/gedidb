import requests
from bs4 import BeautifulSoup
import yaml

class GediMetaDataExtractor:
    def __init__(self, url: str, output_file: str):
        """
        Initialize the GediDataExtractor with a URL and an output file path.

        :param url: The URL of the page to scrape.
        :param output_file: The path to save the extracted data as a YAML file.
        """
        self.url = url
        self.output_file = output_file
        self.soup = None

    def fetch_html_content(self):
        """
        Fetch the HTML content from the given URL.
        """
        response = requests.get(self.url)
        if response.status_code != 200:
            raise ConnectionError(f"Failed to fetch page content. Status code: {response.status_code}")
        self.soup = BeautifulSoup(response.content, 'html.parser')

    def locate_table(self, header_text: str):
        """
        Locate a table in the HTML content based on a header text.
        
        :param header_text: The text of the header preceding the table.
        :return: The BeautifulSoup table element.
        """
        tables = self.soup.find_all("table")
        for table in tables:
            if header_text in table.find_previous("h3").text:
                return table
        if header_text == "Layers / Variables":
            return self.locate_layers_table()
        raise ValueError(f"Could not locate the table with header '{header_text}'.")

    def locate_layers_table(self):
        """
        Locate the "Layers / Variables" table in the HTML content.
        
        :return: The BeautifulSoup table element.
        """
        tables = self.soup.find_all("table")
        for table in tables:
            if "SDS Name" in table.text:
                return table
        raise ValueError("Could not locate the 'Layers / Variables' table.")

    def extract_table_data(self, table):
        """
        Extract data from a given table element.

        :param table: The BeautifulSoup table element.
        :return: A dictionary containing the table data.
        """
        data = {}
        for row in table.find_all("tr")[1:]:  # Skip the header row
            cols = row.find_all("td")
            if len(cols) == 2:  # Ensure the row has the expected number of columns
                key = cols[0].text.strip()
                value = cols[1].text.strip()
                data[key] = value
        return data

    def extract_layers_data(self, table):
        """
        Extract the layers/variables data from the table.

        :param table: The BeautifulSoup table element.
        :return: A list of dictionaries containing the layers data.
        """
        layers_data = []
        for row in table.find_all("tr")[1:]:  # Skip the header row
            cols = row.find_all("td")
            if len(cols) == 8:  # Ensure the row has the expected number of columns
                layer_info = {
                    "SDS_Name": cols[0].text.strip(),
                    "Description": cols[1].text.strip(),
                    "Units": cols[2].text.strip(),
                    "Data_Type": cols[3].text.strip(),
                    "Fill_Value": cols[4].text.strip(),
                    "No_Data_Value": cols[5].text.strip(),
                    "Valid_Range": cols[6].text.strip(),
                    "Scale_Factor": cols[7].text.strip(),
                }
                layers_data.append(layer_info)
        return layers_data

    def save_to_yaml(self, data):
        """
        Save the extracted data to a YAML file.

        :param data: The data to save.
        """
        with open(self.output_file, "w") as file:
            yaml.dump(data, file, default_flow_style=False)
        print(f"Config file saved successfully to {self.output_file}!")

    def run(self):
        """
        Execute the entire extraction process for both the Layers/Variables and Collection/Granule data.
        """
        self.fetch_html_content()

        # Extracting Collection data
        collection_table = self.locate_table("Collection")
        collection_data = self.extract_table_data(collection_table)

        # Extracting Granule data
        granule_table = self.locate_table("Granule")
        granule_data = self.extract_table_data(granule_table)

        # Extracting Layers/Variables data
        layers_table = self.locate_table("Layers / Variables")
        layers_data = self.extract_layers_data(layers_table)

        # Combine all data
        data = {
            "Collection": collection_data,
            "Granule": granule_data,
            "Layers_Variables": layers_data
        }

        self.save_to_yaml(data)
