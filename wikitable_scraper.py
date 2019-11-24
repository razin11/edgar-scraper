# -*- coding: utf-8 -*-
"""
Created on Thu Jul 19 19:20:05 2018

@author: ABMRazin
"""

import urllib.request, urllib.parse, urllib.error
from bs4 import BeautifulSoup
import pandas as pd

x1 = []
x2 = []
x3 = []
x4 = []
x5 = []
x6 = []
x7 = []
x8 = []
x9 = []
x10 = []
column_lst = []

def wikitable_scraping(url, table_index):
    
    
    """
    Finds a table in a wikipedia page when provided the url and table index. Used to scrape all 
    S&P 500 companies along with the basic information of the companies and cik number
    """
    
    html = urllib.request.urlopen(url).read()
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")[table_index]
    for data_1 in tables.find_all("tr"):
        try:
            col_td = data_1.find_all("td")
            col1 = col_td[0].text.rstrip()
            x1.append(col1)
            col2 = col_td[1].text.rstrip()
            x2.append(col2)
            col3 = col_td[2].text.rstrip()
            x3.append(col3)
            col4 = col_td[3].text.rstrip()
            x4.append(col4)
            col5 = col_td[4].text.rstrip()
            x5.append(col5)
            col6 = col_td[5].text.rstrip()
            x6.append(col6)
            col7 = col_td[6].text.rstrip()
            x7.append(col7)
            col8 = col_td[7].text.rstrip()
            x8.append(col8)
            col9 = col_td[8].text.rstrip()
            x9.append(col9)
            col10 = col_td[9].text.rstrip()
            x10.append(col10)
        except:
            continue
    for data_2 in tables.find_all("tr"):
        try:
            col_th = data_2.find_all("th")
            if (len(col_th) > 0):
                col_th = col_th[:]
                for columns in col_th:
                    column = columns.text.rstrip()
                    column_lst.append(column)
        except:
            continue
        
    return x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, column_lst

wikitable_scraping("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", 0)
#len(x1)
#len(x7)

def wiki_table(url, table_index):
    
    
    """
    Takes the lists and put them in a dataframe
    """
    
    
    wikitable_scraping(url, table_index)
    df = pd.DataFrame()
    if (len(x1) < 1):
        pass 
    else:
        df[column_lst[0]] = x1
    if (len(x2) < 1):
        pass 
    else:
        df[column_lst[1]] = x2
    if (len(x3) < 1):
        pass 
    else:
        df[column_lst[2]] = x3
    if (len(x4) < 1):
        pass 
    else:
        df[column_lst[3]] = x4
    if (len(x5) < 1):
        pass 
    else:
        df[column_lst[4]] = x5
    if (len(x6) < 1):
        pass
    else:
        df[column_lst[5]] = x6
    if (len(x7) < 1):
        pass 
    else:
        df[column_lst[6]] = x7
    if (len(x8) < 1):
        pass 
    else:
        df[column_lst[7]] = x8
    if (len(x9) < 1):
        pass
    else:
        df[column_lst[8]] = x9
    if (len(x10) < 1):
        pass 
    else:
        df[column_lst[9]] = x10
    
    return df

#table = wiki_table("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", 0)	
#table
#
#table.set_index("Ticker symbol")
#table_cik = table["CIK"]
##table_cik
#
#cik_lst = []
#for cik in table_cik:
#    cik_lst.append(cik)
#        
#print (cik_lst)
#len(cik_lst)
    
    









