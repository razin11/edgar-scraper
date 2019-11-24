# -*- coding: utf-8 -*-
"""
Created on Wed May  1 16:59:03 2019

@author: razin.hussain
"""

import pandas as pd 
import numpy as np
from bs4 import BeautifulSoup
from selenium import webdriver
import time


cd C:\Users\ABMRazin\Documents\web_scrape_projects

def cik_corrector(x):
    x = str(x)
    if len(x) == 4:
       x = "000000" + x
    elif len(x) == 5:
       x = "00000" + x
    elif len(x) == 6:
       x = "0000" + x
    elif len(x) == 7:
       x = "000" + x
    elif len(x) == 8:
       x = "00" + x
    elif len(x) == 9:
       x = "0" + x
    elif len(x) == 10:
       x = x 
    
    return x

def cik():
    df = pd.read_excel("cik_ticker.xlsx", sheet_name = "sheet1")
    df["CIK"] = df["CIK"].apply(lambda x: cik_corrector(x))
    cik_lst = df["CIK"].tolist()
    return cik_lst, df


def fs_links(urlname):
    driver = webdriver.Firefox()
    # get web page
    driver.get(urlname)
    # execute script to scroll down the page
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);var lenOfPage=document.body.scrollHeight;return lenOfPage;")
    # sleep for 30s
    time.sleep(10)
    
    results = driver.find_elements_by_xpath("//*[@class='accordion']//*[contains(@id, 'r2')]")
    
#    statement_name = results.text
#    link = results
#    
    
    print ("Number of elements: ", len(results))
    
    data = []
    for result in results:
        statement_name = result.text
        link = result.find_element_by_tag_name("a")
        statement_link = link.get_attribute("href")
        
        data.append({"statement": statement_name, "link": statement_link})
    
    return data

x = fs_links("https://www.sec.gov/cgi-bin/viewer?action=view&cik=66740&accession_number=0001558370-19-003408&xbrl_type=v#")
print (x)
        
    
    
    
    bs = driver.find_elements_by_xpath("//*[@class='accordion']//*[contains(@id='r4')]")
    cfs = driver.find_elements_by_xpath("//*[@class='accordion']//*[contains(@id='r6')]")
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    





