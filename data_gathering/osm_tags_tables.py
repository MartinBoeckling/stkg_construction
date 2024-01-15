"""
Title: Scraper of OSM main tags documented in 
"""
# import packages
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pandas as pd

# setup selenium parameters to simulate browser due to JavaScript elements
chrome_options = Options()
chrome_options.add_argument("--headless")
driver = webdriver.Chrome(options=chrome_options)
# retrieve web pages with configured selenium driver
driver.get("https://wiki.openstreetmap.org/wiki/Map_features")
osm_wiki_datadata = driver.page_source
driver.quit()
# parse data into a table structure
# parse html page into BS4 to access features
soup = BeautifulSoup(osm_wiki_datadata, 'html.parser')
# extract all elements which are tagged as table in HTML
wiki_tables = soup.find_all("table")
osm_tag_tables = []
# construct pandas dataframe from tables displayed on OpenStreetMap wiki
# iterate over all table tags extracted from the web page
for wiki_table in wiki_tables:
    """
    Process for constructing the follows the separation of the extraction of the table columns as also
    
    """
    column_name = [column_element.get_text().strip() for column_element in wiki_table.find_all('th')]
    table_dataset = []
    for row in wiki_table.find_all('tr')[1:]:
        row_data = dict(zip(column_name, [element.get_text().strip() for element in row.find_all('td')]))
        table_dataset.append(row_data)
    osm_tag_table = pd.DataFrame.from_records(table_dataset)
    osm_tag_table = osm_tag_table.rename(mapper={'Comment': 'Description'}, axis=1)
    if 'Key' not in osm_tag_table.columns:
        osm_tag_table[['Key', 'Value']] = osm_tag_table['Value'].str.split('=', expand=True)
    
    osm_tag_table = osm_tag_table[['Key', 'Value', 'Description']]
    osm_tag_tables.append(osm_tag_table)

osm_tags_overview = pd.concat(osm_tag_tables, axis=0, ignore_index=True)
osm_tags_overview['Value'] = osm_tags_overview['Value'].str.split(' / ')
osm_tags_overview = osm_tags_overview.explode('Value', ignore_index=True)
osm_tags_overview = osm_tags_overview.dropna()
osm_tags_overview.to_parquet('/ceph/mboeckli/stkg/helper/osm_main_tags.parquet', index=False)