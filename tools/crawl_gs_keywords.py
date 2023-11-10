# pylint: skip-file
# type: ignore
import requests
from bs4 import BeautifulSoup
import re
import yaml

if __name__ == "__main__":
    # URL of PostGIS special functions index
    url = "https://postgis.net/docs/manual-1.5/ch08.html"

    # Send an HTTP GET request to the URL
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the HTML content of the page
        soup = BeautifulSoup(response.text, "html.parser")

        # Find all list items within the page
        list_items = soup.find_all("li")

        # Initialize an empty list to store the extracted keywords
        keywords = []

        # Define a regular expression pattern to match the keywords
        keyword_pattern = r"^(.*?)\s-\s"

        # Iterate over each list item and extract the keyword
        for item in list_items:
            text = item.get_text()
            match = re.search(keyword_pattern, text)
            if match:
                keyword = match.group(1).strip()
                keywords.append(keyword)

        # Define the output YAML file name
        output_yaml_file = "postgis_keywords.yml"

        # Write the extracted keywords to a YAML file
        with open(output_yaml_file, "w") as yaml_file:
            yaml.dump(keywords, yaml_file, default_flow_style=False)

        print(f"Extracted {len(keywords)} keywords and saved to {output_yaml_file}")
    else:
        print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
