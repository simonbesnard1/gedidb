# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import requests
from bs4 import BeautifulSoup
import yaml
import logging
from retry import retry
from requests.exceptions import HTTPError


# Configure logging
logger = logging.getLogger(__name__)

def extract_layers_l2data(table):
    """
    Extract the layers/variables data from the L2 table.

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

class GEDIMetaDataDownloader:
    """
    A class to download and extract metadata for GEDI products (L2A, L2B, L4A, L4C) from a given URL.

    The extracted data is saved to a specified YAML file.
    """
    
    def __init__(self, url: str, output_file: str, data_type: str):
        """
        Initialize the GEDIMetaDataDownloader with a URL and an output file path.

        :param url: The URL of the page to scrape.
        :param output_file: The path to save the extracted data as a YAML file.
        :param data_type: The type of data to extract ('L2A', 'L2B', 'L4A', 'L4C').
        """
        self.url = url
        self.output_file = output_file
        self.data_type = data_type
        self.soup = None
    
    def fetch_html_content(self):
        """
        Fetch the HTML content from the specified URL.
        """
        try:
            response = requests.get(self.url)
            response.raise_for_status()
            self.soup = BeautifulSoup(response.content, 'html.parser')
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch page content from {self.url}: {e}")
            raise

    def locate_table(self, header_text: str):
        """
        Locate the relevant table in the HTML content based on the data type (L2A, L4A, L4C).

        :param header_text: The text of the header preceding the table.
        :return: A BeautifulSoup table element.
        """
        if self.data_type in ['L2A', 'L2B']:
            return self.locate_table_l2(header_text)
        elif self.data_type in ['L4A', 'L4C']:
            return self.locate_table_l4(header_text)

    def locate_table_l2(self, header_text: str):
        """
        Locate the table for L2A/L2B in the HTML content based on the header text.

        :param header_text: The text of the header preceding the table.
        :return: The BeautifulSoup table element.
        """
        try:
            tables = self.soup.find_all("table")
            for table in tables:
                if header_text in table.find_previous("h3").text:
                    return table
            if header_text == "Layers / Variables":
                return self.locate_layers_table()
        except Exception as e:
            logger.error(f"Error locating table with header '{header_text}': {e}")
            raise ValueError(f"Could not locate the table with header '{header_text}'.")

    def locate_table_l4(self, header_text: str):
        """
        Locate all tables for L4A/L4C data based on the header text.

        :param header_text: The text of the header preceding the tables.
        :return: A list of BeautifulSoup table elements.
        """
        try:
            tables = self.soup.find_all("table")
            matched_tables = []
            
            for table in tables:
                # Find the previous h2 element
                previous_h2 = table.find_previous("h2")
                
                # Check if the h2 element exists and has text
                if previous_h2 and previous_h2.text and header_text in previous_h2.text:
                    matched_tables.append(table)
            
            if not matched_tables:
                raise ValueError(f"Could not locate any tables with header '{header_text}'.")
            
            return matched_tables
        except Exception as e:
            logger.error(f"Error locating L4 tables with header '{header_text}': {e}")
            raise

    def locate_layers_table(self):
        """
        Locate the "Layers / Variables" table in the HTML content.

        :return: The BeautifulSoup table element.
        """
        try:
            tables = self.soup.find_all("table")
            for table in tables:
                if "SDS Name" in table.text:
                    return table
            raise ValueError("Could not locate the 'Layers / Variables' table.")
        except Exception as e:
            logger.error(f"Error locating Layers / Variables table: {e}")
            raise

    @staticmethod
    def extract_table_data(table):
        """
        Extract data from a given table element.

        :param table: The BeautifulSoup table element.
        :return: A dictionary containing the table data.
        """
        data = {}
        for row in table.find_all("tr")[1:]:  # Skip the header row
            cols = row.find_all("td")
            if len(cols) == 2:
                key = cols[0].text.strip()
                value = cols[1].text.strip()
                data[key] = value
        return data

    @staticmethod
    def extract_layers_l4data(table):
        """
        Extract the layers/variables data from the L4 table.

        :param table: The BeautifulSoup table element.
        :return: A list of dictionaries containing the layers data.
        """
        layers_data = []
        for row in table.find_all("tr")[1:]:  # Skip the header row
            cols = row.find_all("td")
            if len(cols) == 3:
                layer_info = {
                    "SDS_Name": cols[0].text.strip(),
                    "Units": cols[1].text.strip(),
                    "Description": cols[2].text.strip()
                }
                layers_data.append(layer_info)
        return layers_data

    def save_to_yaml(self, data):
        """
        Save the extracted data to a YAML file.

        :param data: The data to save.
        """
        try:
            with open(self.output_file, "w") as file:
                yaml.dump(data, file, default_flow_style=False)
        except IOError as e:
            logger.error(f"Failed to write data to {self.output_file}: {e}")
            raise

    @retry((ValueError, TypeError, HTTPError), tries=10, delay=5, backoff=3)
    def build_metadata(self):
        """
        Execute the entire extraction process for both the Layers/Variables and Collection/Granule data.
        """
        try:
            self.fetch_html_content()

            if self.data_type in ["L2A", "L2B"]:
                collection_table = self.locate_table("Collection")
                collection_data = self.extract_table_data(collection_table)

                granule_table = self.locate_table("Granule")
                granule_data = self.extract_table_data(granule_table)

                layers_table = self.locate_table("Layers / Variables")
                layers_data = extract_layers_l2data(layers_table)

                data = {
                    "Collection": collection_data,
                    "Granule": granule_data,
                    "Layers_Variables": layers_data
                }

            elif self.data_type in ["L4A", "L4C"]:
                layers_tables = self.locate_table('Data Characteristics')
                layers_data = []
                for table in layers_tables:
                    layers_data.extend(self.extract_layers_l4data(table))

                data = {
                    "Layers_Variables": layers_data
                }

            self.save_to_yaml(data)
        except Exception as e:
            logger.error(f"Failed to build metadata for {self.data_type}: {e}")
            raise
