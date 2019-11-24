# -*- coding: utf-8 -*-
"""
Created on Sun Nov 24 08:57:45 2019

@author: ABMRazin
"""

# Setup file for the edgar_scraper_xbrl

import pymysql
from  sqlalchemy import create_engine

conn = pymysql.connect(host = "localhost", user = "root", passwd = "13Tallabagh")
cur = conn.cursor()

cur.execute("create database fd_trial")
cur.execute("use fd_trial")

engine = create_engine("mysql+pymysql://root: 13Tallabagh@localhost: 3306/fd_trial")

# cd "path to where you have saved wikitable_scraper file"
cd C:\Users\ABMRazin\Documents\Cousera\py4e\webscraping_practice\wikitable_scraper
import wikitable_scraper as ws

def sp500_database():
    
    
#    The function calls the wikitable_scraper, keeps relevant data required for the project and
#    imports the dataframe directly to the database created above
    
    
    table = ws.wiki_table("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", 0)
    table["id"] = [i for i in range (len(table))]

    cols_to_keep = ["id", "Symbol", "Security", "GICS Sector", "GICS Sub Industry", "Headquarters Location", "CIK"]
    table = table[cols_to_keep]

    table.columns = ["id", "ticker", "security", "sector", "sub_sector", "headquarter", "CIK"]
    
    df.to_sql(name = "symbol", con = engine, if_exists = "replace", index = False)
    
    return table

df = sp500_database()

###### Things to note #######
# The pupose of this .py file is to create a table that include metadata for all S&P 500 companies 
# and import to the database. However, MySQL needs to be setup before the import can be done.
# Also note wikitable_scraper need to be in a valid location and the path needs to be assigned
# For the database, creating the engine object is critical for direct importation of the dataframe to database.
# create_engine module from sqlalchemy is used for this purpose, which requires specific path identification
# Format for create_engine:
    # engine = create_engine("mysql+pymysql://[user]: [passwd]@[host]: [port]/[database_name]") 
