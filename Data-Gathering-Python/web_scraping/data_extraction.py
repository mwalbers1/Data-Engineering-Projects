import os
import pandas as pd
import numpy as np
import json
import re
import requests
from bs4 import BeautifulSoup
import wget
import csv


def get_links_from_page(start_url):
    """
    Get list of links from web page
    
    args:
        start_url: Url of web page to extract href elements
        
    return:
        list of hrefs on web page
    """
    # Download the HTML from start_url
    downloaded_html = requests.get(start_url)
    
    # Parse the HTML with BeautifulSoup and create a soup object
    soup = BeautifulSoup(downloaded_html.text, features="lxml")
    
    # Get list of links from web page
    return [a['href'] for a in soup.find_all('a', href=True) if a.text]
    
            
def get_filename_href(links_with_href, url_prefix):
    """
    Iterate through list of href links for given url_prefix and parse out filename and href location

    args:
        links_with_href: list of href links pointing to csv file
        url_prefix: url prefix where links with hrefs is located
    
    returns: tuple containing csv filename and href for csv file
    """
    expr1 = '^.*\.(csv|CSV)$'
    p = re.compile(expr1)
    
    indicator_list = []
    file_href_list = []

    for l in links_with_href:
        b = p.match(l)
        if b:
            file_name = l.split('/')[-1]
            file_href2 = l.split('/')[-2]
            file_href3 = l.split('/')[-3]
            file_href_final = f"{url_prefix}{file_href3}/{file_href2}/{file_name}"
            yield file_name,file_href_final


def download_csv_file(url_input_file):
    """
    Read a csv file from the url_input_file argument. Then download csv file using wget
    
    args: 
        url_input_file: href to a specific csv file
    """
    filename = wget.download(url_input_file, './data')
    print(f"Downloaded {filename}")
