# -*- coding: utf-8 -*-
"""
Created on Tue May 14 11:03:53 2019

@author: razin.hussain
"""

import pandas as pd
import numpy as np
import urllib.request, urllib.parse, urllib.error
from bs4 import BeautifulSoup
import re
import pymysql
from  sqlalchemy import create_engine
import lxml
import xml.etree.ElementTree as ET
import json
import datetime
import time

from functools import reduce

conn = pymysql.connect(host = "", user = "", passwd = "")
cur = conn.cursor()

## Only used when creating the database
# cur.execute("create database financial_database")

# Using the cursor object created above to use the database file financial_database1
cur.execute("use financial_database1")

#cur.execute("drop table if exists symbol")
#cur.execute("create table symbol (id INTEGER PRIMARY KEY AUTO_INCREMENT UNIQUE, ticker VARCHAR (64), security VARCHAR(255), sector VARCHAR (64), sub_sector VARCHAR(255), headquarter VARCHAR (255), CIK VARCHAR(255))")
 
 # Create an engine object to import dataframe directly to the database
engine = create_engine("")


def sp500_cik():
    
    
    """
    The function grabs stock IDs, symbols and CIKs from the database
    and put them in a list
    """
    
    
    df = pd.read_sql_table("symbol", con = engine)
    cik_lst = []
    ticker_lst = []
    id_lst = []
    for cik in df["CIK"]:
        cik_lst.append(cik)
    for ticker in df["ticker"]:
        ticker_lst.append(ticker)
    for id in df["id"]:
        id_lst.append(id)
    
    return cik_lst, ticker_lst, id_lst


def first_scraper(urlname1):
    
    
    """
    The function takes a url as a parameter, (example url:
    https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000764180&type=10-K&dateb=&owner=exclude&count=40)
    extract the urls of 10Ks and 10Qs from the initial page
    and convert it into executable urls and append them in a list
    """
    
    
    filings = []
    url_lst = []
    html1 = urllib.request.urlopen(urlname1).read()
    soup1 = BeautifulSoup(html1, 'html.parser')
    for tr1 in soup1.find_all("tr")[2:]:
        try:
            tds1 = tr1.find_all("td")
            document_type = tds1[0].text.rstrip()
            if document_type == "10-Q" or document_type == "10-K":
                filings.append(document_type)
                hrefs1 = tds1[1].findChildren()
                hrefs1 = hrefs1[0].get("href")
                base_url1 = "https://www.sec.gov"
                final_url1 = base_url1 + hrefs1
                url_lst.append(final_url1)
        except:
            continue
    
    return filings, url_lst

#x = first_scraper("https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000764180&type=10-K&dateb=&owner=exclude&count=40")

def second_scraper(urlname2):
    
    
    """
    The function takes a url as a parameter, (examle url: 
    https://www.sec.gov/Archives/edgar/data/55785/000005578519000032/0000055785-19-000032-index.htm)
    extracts the xbrl_link, report date and report period from the page and returns them as string
    """
        
        
    html2 = urllib.request.urlopen(urlname2).read()
    soup2 = BeautifulSoup(html2, 'html.parser')
    try:
        table = soup2.find("table", {"class": "tableFile", "summary": "Data Files"})
        rows = table.find_all("tr")
    except:
        return 0
    for row in rows:
        tds = row.find_all("td")
        if len(tds) > 3:
            if "INS" in tds[3].text or "XML" in tds[3].text:
                xbrl_link = tds[2].a["href"]
                xbrl_link = "https://www.sec.gov" + xbrl_link
            
#        if len(tds) > 3:
#            if "DEF" in tds[3].text:
#                desc_link = tds[2].a["href"]
#                desc_link = "https://www.sec.gov" + desc_link

    div = soup2.find("div", {"class":"formContent"})
    div1 = div.find_all("div", {"class":"formGrouping"})[0]
    report_date = div1.find_all("div", {"class":"info"})[0].text.rstrip()
    div2 = div.find_all("div", {"class":"formGrouping"})[1]
    report_period = div2.find("div", {"class":"info"}).text.rstrip()

    return xbrl_link, report_date, report_period

# x = second_scraper("https://www.sec.gov/Archives/edgar/data/1551152/000155115219000008/0001551152-19-000008-index.htm")

#d1 = "2016-10-01"
#d2 = "2017-06-30"
#d1 = datetime.datetime.strptime(d1, "%Y-%m-%d")
#d2 = datetime.datetime.strptime(d2, "%Y-%m-%d")
#
#days = d2 - d1
#print (days.days)
# dei_lst = ["{http://xbrl.sec.gov/dei/2009-01-31}", "{http://xbrl.sec.gov/dei/2010-01-31}", "{http://xbrl.sec.gov/dei/2011-01-31}", "{http://xbrl.sec.gov/dei/2012-01-31}", "{http://xbrl.sec.gov/dei/2013-01-31}", "{http://xbrl.sec.gov/dei/2014-01-31}", "{http://xbrl.sec.gov/dei/2015-01-31}", "{http://xbrl.sec.gov/dei/2016-01-31}", "{http://xbrl.sec.gov/dei/2017-01-31}", "{http://xbrl.sec.gov/dei/2018-01-31}", "{http://xbrl.sec.gov/dei/2019-01-31}"]
           

def xbrl_scraper(urlname2, report_type):


    """
    The heart of the program. The function xbrl_scraper takes the financial statement xml file url
    and type of report as arguments, which has already been scraped using the previous two functions.
    It initates a master dictionary that include three other dictionaries (branch dictionaries) - 
    IS dictionary, BS dictionary and CFS dictionary. The function than searches for income statement, 
    balance sheet and cash flow statement items in that order for the specific quarter/year the 
    document is representing and update the respective dictionaries once the items are found. 
    When a branch dictionary is completed, master dictioanry is updated with branch. 
    """" 
    
    
    # Calls the second_scraper function - see description above"
    file_page = second_scraper(urlname2)
    try:
        xbrl_link = file_page[0]
    except:
        print ("XBRL format does not exist")
        return 0
    
    # Opens the xml file and creates and creates a root object that allows access to initial tag in the xml file  
    xml = urllib.request.urlopen(xbrl_link)
    tree = ET.parse(xml)
    root = tree.getroot()
    master_dct = {}
    
    # Unique dei list for a particular year generic to all companies
    dei_lst = ["{http://xbrl.sec.gov/dei/2009-01-31}", "{http://xbrl.sec.gov/dei/2010-01-31}", "{http://xbrl.sec.gov/dei/2011-01-31}", "{http://xbrl.sec.gov/dei/2012-01-31}", "{http://xbrl.sec.gov/dei/2013-01-31}", "{http://xbrl.sec.gov/dei/2014-01-31}", "{http://xbrl.sec.gov/dei/2015-01-31}", "{http://xbrl.sec.gov/dei/2016-01-31}", "{http://xbrl.sec.gov/dei/2017-01-31}", "{http://xbrl.sec.gov/dei/2018-01-31}", "{http://xbrl.sec.gov/dei/2019-01-31}"]
    for dei in dei_lst:
        try:
            period_code = "DocumentFiscalPeriodFocus"
            period_code = dei + period_code
            period = root.find(period_code).text
            
            year_code = "DocumentFiscalYearFocus"
            year_code = dei + year_code
            year = root.find(year_code).text
            
            period_code = "DocumentPeriodEndDate"
            period_code = dei + period_code
            report_period = root.find(period_code).text
            print (report_period)
            
            quarter = period + year
            print (quarter)
            
            shares_code = "EntityCommonStockSharesOutstanding"
            shares_code = dei + shares_code
            shares_os = root.find(shares_code).text
            print ("Common shares O/S:", shares_os)

        except:
            continue
    
    
    for context in root.iter("{http://www.xbrl.org/2003/instance}context"):
        try:
            entity = context.find("{http://www.xbrl.org/2003/instance}entity")
            cik = entity.find("{http://www.xbrl.org/2003/instance}identifier").text
            print ("CIK:", cik)
            break
        except:
            continue
        
    is_dct = {}
    
    # Find the spefic id attribute that contains the date code for the current quarter
    # INCOME STATEMENT Scraper
    for context in root.iter("{http://www.xbrl.org/2003/instance}context"):
        if "Q" in context.get("id") and "D" in context.get("id") and "_" not in context.get("id") and "YTD" not in context.get("id"):
#            print ("First if worked")
            period = context.find("{http://www.xbrl.org/2003/instance}period")
            try:
                start_date = period.find("{http://www.xbrl.org/2003/instance}startDate").text
                start_date_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
                end_date = period.find("{http://www.xbrl.org/2003/instance}endDate").text
                end_date_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
                delta = end_date_dt - start_date_dt
                days = delta.days
                print ("No. of days:", days)
                
                if report_type == "10-Q":
                    if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period and days < 120 and days != 0:
                        # print ("Second if worked")
                        date_code = context.get("id")
                        print ("IS 1st method:", date_code)
                elif report_type == "10-K":
                    if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period and days > 280:
                        # print ("Second if worked")
                        date_code = context.get("id")
                        print ("IS 1st method:", date_code)                    
            except:
                continue

# 
        elif "Y" in context.get("id") and "D" in context.get("id") and "_" not in context.get("id"):
#            print ("First if worked")
            period = context.find("{http://www.xbrl.org/2003/instance}period")
            try:
                start_date = period.find("{http://www.xbrl.org/2003/instance}startDate").text
                start_date_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
                end_date = period.find("{http://www.xbrl.org/2003/instance}endDate").text
                end_date_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
                delta = end_date_dt - start_date_dt
                days = delta.days
                print ("No. of days:", days)                
                
                if report_type == "10-Q":
                    if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period and days < 120 and days != 0:
                        # print ("Second if worked")
                        date_code = context.get("id")
                        print ("IS 2nd method:", date_code)
                elif report_type == "10-K":
                    if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period and days > 280:
                        # print ("Second if worked")
                        date_code = context.get("id")
                        print ("IS 2nd method:", date_code)
            except:
                continue

        elif "D" in context.get("id") and report_period[0:4] in context.get("id") and "_" not in context.get("id"):
#            print ("First if worked")
            period = context.find("{http://www.xbrl.org/2003/instance}period")
            try:
                start_date = period.find("{http://www.xbrl.org/2003/instance}startDate").text
                start_date_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
                end_date = period.find("{http://www.xbrl.org/2003/instance}endDate").text
                end_date_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
                delta = end_date_dt - start_date_dt
                days = delta.days
                print ("No. of days:", days)                
                
                if report_type == "10-Q":
                    if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period and days < 120 and days != 0:
                        # print ("Second if worked")
                        date_code = context.get("id")
                        print ("IS 3rd method:", date_code)
                elif report_type == "10-K":
                    if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period and days > 280:
                        # print ("Second if worked")
                        date_code = context.get("id")
                        print ("IS 3rd method:", date_code)
            except:
                continue

        elif cik in context.get("id") and "".join(report_period.split("-")) in context.get("id") and "us-gaap" not in context.get("id"):
#            print ("First if worked")
            period = context.find("{http://www.xbrl.org/2003/instance}period")
            try:
                start_date = period.find("{http://www.xbrl.org/2003/instance}startDate").text
                start_date_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
                end_date = period.find("{http://www.xbrl.org/2003/instance}endDate").text
                end_date_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
                delta = end_date_dt - start_date_dt
                days = delta.days
                print ("No. of days:", days)
                
                if report_type == "10-Q":
                    if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period and days < 120 and days != 0:
                        # print ("Second if worked")
                        date_code = context.get("id")
                        print ("IS 4th method:", date_code)
                elif report_type == "10-K":
                    if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period and days > 280:
                        # print ("Second if worked")
                        date_code = context.get("id")
                        print ("IS 4th method:", date_code)
            except:
                continue
        
        elif "Duration" in context.get("id") and "us-gaap" not in context.get("id") and "dei" not in context.get("id") and "srt" not in context.get("id"):
            period = context.find("{http://www.xbrl.org/2003/instance}period")
            try:
                start_date = period.find("{http://www.xbrl.org/2003/instance}startDate").text
                start_date_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
                end_date = period.find("{http://www.xbrl.org/2003/instance}endDate").text
                end_date_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
                delta = end_date_dt - start_date_dt
                days = delta.days
                print ("No. of days:", days)
                
                if report_type == "10-Q":
                    if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period and days < 120 and days != 0:
                        # print ("Second if worked")
                        date_code = context.get("id")
                        print ("IS 5th method:", date_code)
                elif report_type == "10-K":
                    if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period and days > 280:
                        # print ("Second if worked")
                        date_code = context.get("id")
                        print ("IS 5th method:", date_code)
            except:
                continue
#[0:29]
#[0:29]
        elif "FROM" in context.get("id") and "TO" in context.get("id"):
            period = context.find("{http://www.xbrl.org/2003/instance}period")
            try:
                start_date = period.find("{http://www.xbrl.org/2003/instance}startDate").text
                start_date_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
                end_date = period.find("{http://www.xbrl.org/2003/instance}endDate").text
                end_date_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
                delta = end_date_dt - start_date_dt
                days = delta.days
                print ("No. of days:", days)
                
                if report_type == "10-Q":
                    if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period and days < 120 and days != 0:
                        # print ("Second if worked")
                        date_code = context.get("id")
                        print ("IS 6th method:", date_code)
                elif report_type == "10-K":
                    if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period and days > 280:
                        # print ("Second if worked")
                        date_code = context.get("id")
                        print ("IS 6th method:", date_code)
            except:
                continue        

        elif "eol" in context.get("id") and "".join(report_period.split("-")) in context.get("id") and context.get("id").endswith("0") and "x" not in context.get("id"):
#            print ("First if worked")
            period = context.find("{http://www.xbrl.org/2003/instance}period")
            try:
                start_date = period.find("{http://www.xbrl.org/2003/instance}startDate").text
                start_date_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
                end_date = period.find("{http://www.xbrl.org/2003/instance}endDate").text
                end_date_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
                delta = end_date_dt - start_date_dt
                days = delta.days
                print ("No. of days:", days)
                
                if report_type == "10-Q":
                    if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period and days < 120 and days != 0:
                        # print ("Second if worked")
                        date_code = context.get("id")
                        print ("IS 7th method:", date_code)
                elif report_type == "10-K":
                    if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period and days > 280:
                        # print ("Second if worked")
                        date_code = context.get("id")
                        print ("IS 7th method:", date_code)                    
            except:
                continue
        
#        else:
#            period = context.find("{http://www.xbrl.org/2003/instance}period")
#            try:
#                start_date = period.find("{http://www.xbrl.org/2003/instance}startDate").text
#                start_date_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
#                end_date = period.find("{http://www.xbrl.org/2003/instance}endDate").text
#                end_date_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
#                delta = end_date_dt - start_date_dt
#                days = delta.days
#                print ("No. of days:", days)
#                
#                if report_type == "10-Q":
#                    if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period and days < 120 and days != 0:
#                        date_code = context.get("id")
#                        print ("IS last method:", date_code)
#                elif report_type == "10-K":
#                    if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period and days > 280:
#                        date_code = context.get("id")
#                        print ("IS last method:", date_code)
#            except:
#                continue
        
        # Different varitaions of the income statement items are tested to ensure items are not missed during scraping
        v_lst = ["{http://xbrl.us/us-gaap/2009-01-31}", "{http://xbrl.us/us-gaap/2010-01-31}", "{http://fasb.org/us-gaap/2011-01-31}", "{http://fasb.org/us-gaap/2012-01-31}", "{http://fasb.org/us-gaap/2013-01-31}", "{http://fasb.org/us-gaap/2014-01-31}", "{http://fasb.org/us-gaap/2015-01-31}", "{http://fasb.org/us-gaap/2016-01-31}", "{http://fasb.org/us-gaap/2017-01-31}", "{http://fasb.org/us-gaap/2018-01-31}", "{http://fasb.org/us-gaap/2019-01-31}"]
        sales_lst = ["TotalRevenuesNetOfInterestExpense", "OilAndGasRevenue", "ExplorationAndProductionRevenue", "RegulatedAndUnregulatedOperatingRevenue", "AssetManagementFees1", "RealEstateRevenueNet", "SalesRevenueGoodsNet", "SalesRevenueServicesGross", "SalesRevenueServicesNet", "SalesRevenueNet", "Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "RevenueFromContractWithCustomerIncludingAssessedTax"]
        cos_lst = ["ContractRevenueCost", "CostOfGoodsSoldExcludingDepreciationDepletionAndAmortization", "CostOfGoodsSold", "CostOfServices", "CostOfGoodsAndServicesSold", "CostOfRevenue"]
        sga_lst = ["SellingGeneralAndAdministrativeExpense"]
        ga_lst = ["GeneralAndAdministrativeExpense"]
        marketing_lst =  ["SellingAndMarketingExpense", "AdvertisingExpense"]
        salary_lst = ["LaborAndRelatedExpense"]
        rd_lst = ["CommunicationsAndInformationTechnology", "ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost", "ResearchAndDevelopmentExpense"]
#        distrib_lst = ["DistributionExpenses"]
#        rental_lst = ["DirectCostsOfLeasedAndRentedPropertyOrEquipment"]
#        provloss_lst = ["PolicyholderBenefitsAndClaimsIncurredNet", "ProvisionForLoanLeaseAndOtherLosses"]
        restructure_lst = ["RestructuringCharges"]
        impairexp_lst = ["AmortizationOfIntangibleAssets", "ImpairmentOfRealEstate", "DeferredPolicyAcquisitionCostsAndPresentValueOfFutureProfitsAmortization1","GoodwillAndIntangibleAssetImpairment", "GoodwillImpairmentLoss", "RestructuringSettlementAndImpairmentProvisions"]
        extdebt_lst = ["GainsLossesOnExtinguishmentOfDebt"]
        litigexp_lst = ["LitigationSettlementExpense"]
        opexp_lst = ["OperatingExpenses"]
        totexp_lst = ["BenefitsLossesAndExpenses", "NoninterestExpense", "CostsAndExpenses"]
        oi_lst = ["OperatingIncomeLoss"]                
        intexp_lst = ["InterestExpenseBorrowings", "InterestExpense", "InterestExpenseNet", "InterestIncomeExpenseNet"]
        nonopexp_lst = ["OtherNonoperatingIncomeExpense", "OtherNoninterestExpense"]
        pretaxinc_lst = ["IncomeLossFromOperationsBeforeIncomeTaxExpenseBenefit", "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest", "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments", "IncomeLossFromContinuingOperationsIncludingPortionAttributableToNoncontrollingInterest"]
        tax_lst = ["IncomeTaxExpenseBenefit"]
        ni_lst = ["ProfitLoss", "NetIncomeLoss"]
        eps_lst = ["EarningsPerShareBasic", "EarningsPerShareBasicAndDiluted", "EarningsPerShareDiluted"]
        sharesdiluted_lst = ["CommonStockSharesOutstanding", "WeightedAverageNumberOfShareOutstandingBasicAndDiluted", "WeightedAverageNumberOfSharesOutstandingBasic", "WeightedAverageNumberOfDilutedSharesOutstanding"]
        div_lst = ["CommonStockDividendsPerShareDeclared", "CommonStockDividendsPerShareCashPaid"]

        # Extracting income statement items from the xml file and appending to a list
        # Looping through every version and every variation to access the required tags 
        for version in v_lst:

            for sales_term in sales_lst:
                sales_elem = version + sales_term
                try: 
                    for sales in root.iter(sales_elem):
                        if sales.get("contextRef") == date_code:
                            is_dct.update({"sales": sales.text})
                except:
                    continue
        
            for cos_term in cos_lst:
                cos_elem = version + cos_term 
                try: 
                    for cos in root.iter(cos_elem):
                        if cos.get("contextRef") == date_code:
                            is_dct.update({"costofsales": cos.text})
                except:
                    continue
        
            for oi_term in oi_lst:
                oi_elem = version + oi_term 
                try: 
                    for oi in root.iter(oi_elem):
                        if oi.get("contextRef") == date_code:
                            is_dct.update({"operatingincome": oi.text})
                except:
                    continue
        
            for intexp_term in intexp_lst:
                intexp_elem = version + intexp_term 
                try: 
                    for intexp in root.iter(intexp_elem):
                        if intexp.get("contextRef") == date_code:
                            is_dct.update({"interestexpense": intexp.text})                            

                except:
                    continue
        
            for tax_term in tax_lst:
                tax_elem = version + tax_term 
                try: 
                    for tax in root.iter(tax_elem):
                        if tax.get("contextRef") == date_code:
                            is_dct.update({"incometax": tax.text})
                except:
                    continue
        
            for ni_term in ni_lst:
                ni_elem = version + ni_term 
                try: 
                    for ni in root.iter(ni_elem):
                        if ni.get("contextRef") == date_code:
                            is_dct.update({"netincome": ni.text})
                except:
                    continue
        
            for eps_term in eps_lst:
                eps_elem = version + eps_term 
                try: 
                    for eps in root.iter(eps_elem):
                        if eps.get("contextRef") == date_code:
                            is_dct.update({"gaapdilutedeps": eps.text})
                except:
                    continue
        
            for sharesdiluted_term in sharesdiluted_lst:
                sharesdiluted_elem = version + sharesdiluted_term 
                try: 
                    for sharesdiluted in root.iter(sharesdiluted_elem):
                        if sharesdiluted.get("contextRef") == date_code:
                            is_dct.update({"dilutedsharesos": sharesdiluted.text}) 
                except:
                    try:
                        is_dct.update({"dilutedsharesos": shares_os})
                    except:
                        continue

            for div_term in div_lst:
                div_elem = version + div_term 
                try: 
                    for div in root.iter(div_elem):
                        if div.get("contextRef") == date_code:
                            is_dct.update({"dps": div.text}) 
                except:
                    continue

            for restructure_term in restructure_lst:
                restructure_elem = version + restructure_term 
                try: 
                    for restructure in root.iter(restructure_elem):
                        if restructure.get("contextRef") == date_code:
                            is_dct.update({"restructuringexpense": restructure.text})
                except:
                    continue

            for impairexp_term in impairexp_lst:
                impairexp_elem = version + impairexp_term 
                try: 
                    for impairexp in root.iter(impairexp_elem):
                        if impairexp.get("contextRef") == date_code:
                            is_dct.update({"impairmentexpense": impairexp.text})
                except:
                    continue

            for extdebt_term in extdebt_lst:
                extdebt_elem = version + extdebt_term 
                try: 
                    for extdebt in root.iter(extdebt_elem):
                        if extdebt.get("contextRef") == date_code:
                            is_dct.update({"extinguishmentdebt": extdebt.text})
                except:
                    continue

            for litigexp_term in litigexp_lst:
                litigexp_elem = version + litigexp_term 
                try: 
                    for litigexp in root.iter(litigexp_elem):
                        if litigexp.get("contextRef") == date_code:
                            is_dct.update({"litigationexpense": litigexp.text})
                except:
                    continue

            for opexp_term in opexp_lst:
                opexp_elem = version + opexp_term 
                try: 
                    for opexp in root.iter(opexp_elem):
                        if opexp.get("contextRef") == date_code:
                            is_dct.update({"operatingexpense": opexp.text})
                except:
                    continue

            for nonopexp_term in nonopexp_lst:
                nonopexp_elem = version + nonopexp_term 
                try: 
                    for nonopexp in root.iter(nonopexp_elem):
                        if nonopexp.get("contextRef") == date_code:
                            is_dct.update({"nonoperatingexpense": nonopexp.text})
                except:
                    continue            

            for pretaxinc_term in pretaxinc_lst:
                pretaxinc_elem = version + pretaxinc_term 
                try: 
                    for pretaxinc in root.iter(pretaxinc_elem):
                        if pretaxinc.get("contextRef") == date_code:
                            is_dct.update({"pretaxincome": pretaxinc.text})
                except:
                    continue        

            for sga_term in sga_lst:
                sga_elem = version + sga_term 
                try: 
                    for sga in root.iter(sga_elem):
                        if sga.get("contextRef") == date_code:
                            is_dct.update({"sellinggeneraladmin": sga.text})
                except:
                    continue 

            for ga_term in ga_lst:
                ga_elem = version + ga_term 
                try: 
                    for ga in root.iter(ga_elem):
                        if ga.get("contextRef") == date_code:
                            is_dct.update({"generaladmin": ga.text})
                except:
                    continue

            for marketing_term in marketing_lst:
                marketing_elem = version + marketing_term 
                try: 
                    for marketing in root.iter(marketing_elem):
                        if marketing.get("contextRef") == date_code:
                            is_dct.update({"marketing": marketing.text})
                except:
                    continue

            for salary_term in salary_lst:
                salary_elem = version + salary_term 
                try: 
                    for salary in root.iter(salary_elem):
                        if salary.get("contextRef") == date_code:
                            is_dct.update({"salary": salary.text})
                except:
                    continue
  
            for rd_term in rd_lst:
                rd_elem = version + rd_term 
                try: 
                    for rd in root.iter(rd_elem):
                        if rd.get("contextRef") == date_code:
                            is_dct.update({"researchdevelopment": rd.text})
                except:
                    continue              

            for totexp_term in totexp_lst:
                totexp_elem = version + totexp_term 
                try: 
                    for totexp in root.iter(totexp_elem):
                        if totexp.get("contextRef") == date_code:
                            is_dct.update({"totalexpense": totexp.text})
                except:
                    continue              


        if len(is_dct) < 5:
            continue
        else:
            new_dct = {"is_datecode": date_code, "report_period": report_period, "report_type": report_type, "start_date": start_date, "days_in_period": days, "quarter": quarter}
            is_dct.update(new_dct)
            break
        break
    
    master_dct.update({"Income Statement": is_dct})

#        if len(datais_lst) < 2:
#            continue
#        else:
#            break
    
#    master_dct.update({"Income Statement": datais_lst})
    
    bs_dct = {}
    
    # Find the spefic id attribute that contains the date code for the current quarter
    # BALANCE SHEET Scraper
    for context in root.iter("{http://www.xbrl.org/2003/instance}context"):
        if "Q" in context.get("id") and "I" in context.get("id") and "_" not in context.get("id"):
#            print ("First if worked")
            period = context.find("{http://www.xbrl.org/2003/instance}period")
            try:
                if period.find("{http://www.xbrl.org/2003/instance}instant").text == report_period:
#                    print ("Second if worked")
                    date_code = context.get("id")
                    print ("BS 1st method:", date_code)
            except:
                continue

        elif "I" in context.get("id") and report_period[0:4] in context.get("id") and "_" not in context.get("id"):
#            print ("First if worked")
            period = context.find("{http://www.xbrl.org/2003/instance}period")
            try:
                if period.find("{http://www.xbrl.org/2003/instance}instant").text == report_period:
#                    print ("Second if worked")
                    date_code = context.get("id")
                    print ("BS 2nd method:", date_code)
            except:
                continue

        
#        elif "YTD" in context.get("id") and "us-gaap" not in context.get("id"):
##            print ("First if worked")
#            period = context.find("{http://www.xbrl.org/2003/instance}period")
#            try:
#                if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period:
##                    print ("Second if worked")
#                    date_code = context.get("id")
#                    print ("2nd method", date_code)
#            except:
#                continue

        # Replace the hardcoded cik number with dynamic cik term
        elif cik in context.get("id") and "".join(report_period.split("-")) in context.get("id") and "us-gaap" not in context.get("id"):
#            print ("First if worked")
            period = context.find("{http://www.xbrl.org/2003/instance}period")
            try:
                if period.find("{http://www.xbrl.org/2003/instance}instant").text == report_period:
#                    print ("Second if worked")
                    date_code = context.get("id")
                    print ("BS 3rd method:", date_code)
            except:
                continue
        
        elif "As_Of" in context.get("id") and "us-gaap" not in context.get("id") and "dei" not in context.get("id") and "srt" not in context.get("id"):
            period = context.find("{http://www.xbrl.org/2003/instance}period")
            if report_type == "10-Q":
                try:
                    if period.find("{http://www.xbrl.org/2003/instance}instant").text == report_period:
                        date_code = context.get("id")
                        print ("BS 4th method:", date_code)
                except:
                    continue
            elif report_type == "10-K":
                try:
                    if period.find("{http://www.xbrl.org/2003/instance}instant").text == report_period:
                        date_code = context.get("id")
                        print ("BS 4th method:", date_code)
                except:
                    continue

#[0:16]
#[0:16]
        elif "AS_OF" in context.get("id"):
            period = context.find("{http://www.xbrl.org/2003/instance}period")
            if report_type == "10-Q":
                try:
                    if period.find("{http://www.xbrl.org/2003/instance}instant").text == report_period:
                        date_code = context.get("id")
                        print ("BS 5th method:", date_code)
                except:
                    continue
            elif report_type == "10-K":
                try:
                    if period.find("{http://www.xbrl.org/2003/instance}instant").text == report_period:
                        date_code = context.get("id")
                        print ("BS 5th method:", date_code)
                except:
                    continue

        elif "eol" in context.get("id") and "".join(report_period.split("-")) in context.get("id") and context.get("id").endswith("0") and "x" not in context.get("id"):
#            print ("First if worked")
            period = context.find("{http://www.xbrl.org/2003/instance}period")
            try:
                if period.find("{http://www.xbrl.org/2003/instance}instant").text == report_period:
#                    print ("Second if worked")
                    date_code = context.get("id")
                    print ("BS 6th method:", date_code)
            except:
                continue
        
#        else:
#            period = context.find("{http://www.xbrl.org/2003/instance}period")
#            try:
#                if period.find("{http://www.xbrl.org/2003/instance}instant").text == report_period:
#                    date_code = context.get("id")
#                    print ("BS last method:", date_code)
#            except:
#                continue
        
        # Different varitaions of the balance sheet items are tested to ensure items are not missed during scraping
        v_lst = ["{http://xbrl.us/us-gaap/2009-01-31}", "{http://xbrl.us/us-gaap/2010-01-31}", "{http://fasb.org/us-gaap/2011-01-31}", "{http://fasb.org/us-gaap/2012-01-31}", "{http://fasb.org/us-gaap/2013-01-31}", "{http://fasb.org/us-gaap/2014-01-31}", "{http://fasb.org/us-gaap/2015-01-31}", "{http://fasb.org/us-gaap/2016-01-31}", "{http://fasb.org/us-gaap/2017-01-31}", "{http://fasb.org/us-gaap/2018-01-31}", "{http://fasb.org/us-gaap/2019-01-31}"]
        cash_lst = ["CashAndCashEquivalentsAtCarryingValue", "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"]
        ar_lst = ["AccountsReceivableNetCurrent", "ReceivablesNetCurrent"]
        inv_lst = ["InventoryNet"]
        ca_lst = ["AssetsCurrent"]
        ppe_lst = ["PropertyPlantAndEquipmentNet"]
        goodw_lst = ["Goodwill"]
        assets_lst = ["Assets"]
        stdebt_lst = ["DebtCurrent", "LongTermDebtCurrent", "ShortTermBankLoansAndNotesPayable", "ShortTermBorrowings"]
        ap_lst = ["AccountsPayableCurrent", "AccountsPayableTradeCurrent"]
        cl_lst = ["LiabilitiesCurrent"]
        ltdebt_lst = ["OtherLongTermDebtNoncurrent", "DebtAndCapitalLeaseObligations", "LongTermDebt", "LongTermDebtNoncurrent", "LongTermDebtAndCapitalLeaseObligations"]
        paidincap_lst = ["AdditionalPaidInCapital", "AdditionalPaidInCapitalCommonStock", "CommonStocksIncludingAdditionalPaidInCapital"]
        retearnings_lst = ["RetainedEarningsAccumulatedDeficit"]
        nc_lst = ["MinorityInterest"]
        equity_lst = ["StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"]
        liabequity_lst = ["LiabilitiesAndStockholdersEquity"]

        # Extracting balance sheet items from the xml file and appending to a list    
        for version in v_lst:
            
            for cash_term in cash_lst:
                cash_elem = version + cash_term
                try: 
                    for cash in root.iter(cash_elem):
                        if cash.get("contextRef") == date_code:
                            bs_dct.update({"cashandcashequivalents": cash.text})                            
                except:
                    continue

            for ar_term in ar_lst:
                ar_elem = version + ar_term
                try:
                    for ar in root.iter(ar_elem):
                        if ar.get("contextRef") == date_code:
                            bs_dct.update({"accountsreceivable": ar.text})                            
                except:
                    continue

            for inv_term in inv_lst:
                inv_elem = version + inv_term
                try: 
                    for inv in root.iter(inv_elem):
                        if inv.get("contextRef") == date_code:
                            bs_dct.update({"inventory": inv.text})                            
                except:
                    continue

            for ca_term in ca_lst:
                ca_elem = version + ca_term
                try:  
                    for ca in root.iter(ca_elem):
                        if ca.get("contextRef") == date_code:
                            bs_dct.update({"currentassets": ca.text})                            
                except:
                    continue

            for ppe_term in ppe_lst:
                ppe_elem = version + ppe_term
                try: 
                    for ppe in root.iter(ppe_elem):
                        if ppe.get("contextRef") == date_code:
                            bs_dct.update({"propertyplantequipment": ppe.text})                            
                except:
                    continue

            for goodw_term in goodw_lst:
                goodw_elem = version + goodw_term
                try: 
                    for goodw in root.iter(goodw_elem):
                        if goodw.get("contextRef") == date_code:
                            bs_dct.update({"goodwill": goodw.text})                            
                except:
                    continue

            for assets_term in assets_lst:
                assets_elem = version + assets_term
                try: 
                    for assets in root.iter(assets_elem):
                        if assets.get("contextRef") == date_code:
                            bs_dct.update({"assets": assets.text})                            
                except:
                    continue

            for stdebt_term in stdebt_lst:
                stdebt_elem = version + stdebt_term
                try: 
                    for stdebt in root.iter(stdebt_elem):
                        if stdebt.get("contextRef") == date_code:
                            bs_dct.update({"shorttermdebt": stdebt.text})                            
                except:
                     continue

            for ap_term in ap_lst:
                ap_elem = version + ap_term
                try: 
                    for ap in root.iter(ap_elem):
                        if ap.get("contextRef") == date_code:
                            bs_dct.update({"accountspayable": ap.text})
                except:
                    continue

            for cl_term in cl_lst:
                cl_elem = version + cl_term
                try: 
                    for cl in root.iter(cl_elem):
                        if cl.get("contextRef") == date_code:
                            bs_dct.update({"currentliabilities": cl.text})
                            # print ("Total Current Liabilities", cl.text)
                except:
                    continue

            for ltdebt_term in ltdebt_lst:
                ltdebt_elem = version + ltdebt_term
                try: 
                    for ltdebt in root.iter(ltdebt_elem):
                        if ltdebt.get("contextRef") == date_code:
                            bs_dct.update({"longtermdebt": ltdebt.text})
                            # print ("Long-term Debt", ltdebt.text)
                except:
                    continue

            for paidincap_term in paidincap_lst:
                paidincap_elem = version + paidincap_term
                try: 
                    for paidincap in root.iter(paidincap_elem):
                        if paidincap.get("contextRef") == date_code:
                            bs_dct.update({"additionalpic": paidincap.text})                            
                except:
                    continue

            for retearnings_term in retearnings_lst:
                retearnings_elem = version + retearnings_term
                try:  
                    for retearnings in root.iter(retearnings_elem):
                        if retearnings.get("contextRef") == date_code:
                            bs_dct.update({"retainedearnings": retearnings.text})                            
                except:
                    continue

            for nc_term in nc_lst:
                nc_elem = version + nc_term
                try: 
                    for nc in root.iter(nc_elem):
                        if nc.get("contextRef") == date_code:
                            bs_dct.update({"ncinterest": nc.text})                            
                except:
                    continue

            for equity_term in equity_lst:
                equity_elem = version + equity_term
                try: 
                    for equity in root.iter(equity_elem):
                        if equity.get("contextRef") == date_code:
                            bs_dct.update({"equity": equity.text})                            
                except:
                    continue

            for liabequity_term in liabequity_lst:
                liabequity_elem = version + liabequity_term
                try:  
                    for liabequity in root.iter(liabequity_elem):
                        if liabequity.get("contextRef") == date_code:
                            bs_dct.update({"liabilitiesequity": liabequity.text})
                except:
                    continue

        if len(bs_dct) < 5:
            continue
        else:
            new_dct = {"bs_datecode": date_code, "report_period": report_period, "report_type": report_type, "quarter": quarter}
            bs_dct.update(new_dct)
            break
        break
        
#        if len(databs_lst) < 2:
#            continue
#        else:
#            break
    
    master_dct.update({"Balance Sheet": bs_dct})
    
#    master_dct.update({"Balance Sheet": databs_lst})

    cfs_dct = {}

#    datacfs_lst = []

#    Find the spefic id attribute that contains the date code for the current quarter
    # CASH FLOW STATEMENT Scraper
    for context in root.iter("{http://www.xbrl.org/2003/instance}context"):
        if "Q" in context.get("id") and "D" in context.get("id") and "us-gaap" not in context.get("id") and "dei" not in context.get("id"):
#            print ("First if worked")
            period = context.find("{http://www.xbrl.org/2003/instance}period")
            try:
                if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period:
#                    print ("Second if worked")
                    date_code = context.get("id")
                    print ("CFS 1st method", date_code)
            except:
                continue

        elif "D" in context.get("id") and report_period[0:4] in context.get("id") and "us-gaap" not in context.get("id") and "dei" not in context.get("id"):
#            print ("First if worked")
            period = context.find("{http://www.xbrl.org/2003/instance}period")
            try:
                if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period:
#                    print ("Second if worked")
                    date_code = context.get("id")
                    print ("CFS 2nd method", date_code)
            except:
                continue

        
        elif "YTD" in context.get("id") and "_" not in context.get("id"):
#            print ("First if worked")
            period = context.find("{http://www.xbrl.org/2003/instance}period")
            try:
                if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period:
#                    print ("Second if worked")
                    date_code = context.get("id")
                    print ("CFS 3rd method:", date_code)
            except:
                continue
                                    
        # Replace the hardcoded cik number with dynamic cik term
        elif cik in context.get("id") and "".join(report_period.split("-")) in context.get("id") and "us-gaap" not in context.get("id"):
#            print ("First if worked")
            period = context.find("{http://www.xbrl.org/2003/instance}period")
            try:
                if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period:
#                    print ("Second if worked")
                    date_code = context.get("id")
                    print ("CFS 4th method:", date_code)
            except:
                continue
        
        elif "Duration" in context.get("id") and "us-gaap" not in context.get("id") and "dei" not in context.get("id") and "srt" not in context.get("id"):
            period = context.find("{http://www.xbrl.org/2003/instance}period")
            if report_type == "10-Q":
                if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period:
                    date_code = context.get("id")
                    print ("CFS 5th method:", date_code)
            elif report_type == "10-K":
                if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period:
                    date_code = context.get("id")
                    print ("CFS 5th method:", date_code)

#[0:29]
#[0:29]
        elif "FROM" in context.get("id") and "TO" in context.get("id"):
            period = context.find("{http://www.xbrl.org/2003/instance}period")
            if report_type == "10-Q":
                if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period:
                    date_code = context.get("id")
                    print ("CFS 6th method:", date_code)
            elif report_type == "10-K":
                if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period:
                    date_code = context.get("id")
                    print ("CFS 6th method:", date_code)

        elif "eol" in context.get("id") and "".join(report_period.split("-")) in context.get("id") and context.get("id").endswith("0") and "x" not in context.get("id"):
#            print ("First if worked")
            period = context.find("{http://www.xbrl.org/2003/instance}period")
            try:
                if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period:
#                    print ("Second if worked")
                    date_code = context.get("id")
                    print ("CFS 7th method:", date_code)
            except:
                continue
        
#        else:
#            period = context.find("{http://www.xbrl.org/2003/instance}period")
#            try:
#                if period.find("{http://www.xbrl.org/2003/instance}endDate").text == report_period:
#                    date_code = context.get("id")
#                    print ("CFS last method:", date_code)
#            except:
#                continue
        
        # Different varitaions of the casg flow statement items are tested to ensure items are not missed during scraping 
        v_lst = ["{http://xbrl.us/us-gaap/2009-01-31}", "{http://xbrl.us/us-gaap/2010-01-31}", "{http://fasb.org/us-gaap/2011-01-31}", "{http://fasb.org/us-gaap/2012-01-31}", "{http://fasb.org/us-gaap/2013-01-31}", "{http://fasb.org/us-gaap/2014-01-31}", "{http://fasb.org/us-gaap/2015-01-31}", "{http://fasb.org/us-gaap/2016-01-31}", "{http://fasb.org/us-gaap/2017-01-31}", "{http://fasb.org/us-gaap/2018-01-31}", "{http://fasb.org/us-gaap/2019-01-31}"]
        da_lst = ["AmortizationOfIntangibleAssets", "AdjustmentForAmortization", "DepreciationNonproduction", "Depreciation", "DepreciationAmortizationAndAccretionNet", "DepreciationAndAmortization", "DepreciationDepletionAndAmortization"]
        sbcomp_lst = ["ShareBasedCompensation"]
        cashop_lst = ["NetCashProvidedByUsedInOperatingActivities", "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"]
        capex_lst = ["PaymentsForCapitalImprovements", "PaymentsToAcquireProductiveAssets", "PaymentsForProceedsFromProductiveAssets", "PaymentsToAcquirePropertyPlantAndEquipment", "PaymentsForConstructionInProcess"]
        acq_lst = ["PaymentsToAcquireBusinessesNetOfCashAcquired", "PaymentsToAcquireBusinessesAndInterestInAffiliates"]
        cashinv_lst = ["NetCashProvidedByUsedInInvestingActivities", "NetCashProvidedByUsedInInvestingActivitiesContinuingOperations"]
        debtissued_lst = ["ProceedsFromIssuanceOfLongTermDebt"]
        debtrepaid_lst = ["RepaymentsOfDebtAndCapitalLeaseObligations", "RepaymentsOfSecuredDebt", "RepaymentsOfLongTermDebt", "RepaymentsOfDebt"]
        equityissued_lst = ["ProceedsFromIssuanceOfCommonStock"]
        equitybought_lst = ["PaymentsForRepurchaseOfCommonStock"]
        cashfin_lst = ["NetCashProvidedByUsedInFinancingActivities", "NetCashProvidedByUsedInFinancingActivitiesContinuingOperations"]
        
        # Extracting chas flow statement items from the xml file and appending to a list
        for version in v_lst:
            
            for da_term in da_lst:
                da_elem = version + da_term
                
                try: 
                    for da in root.iter(da_elem):
                        if da.get("contextRef") == date_code:
                            cfs_dct.update({"da": da.text})
                except:
                    continue
            
            for sbcomp_term in sbcomp_lst:
                sbcomp_elem = version + sbcomp_term
                try: 
                    for sbcomp in root.iter(sbcomp_elem):
                        if sbcomp.get("contextRef") == date_code:
                            cfs_dct.update({"sbcomp": sbcomp.text})
                except:
                    continue    

            for cashop_term in cashop_lst:
                cashop_elem = version + cashop_term
                try: 
                    for cashop in root.iter(cashop_elem):
                        if cashop.get("contextRef") == date_code:
                            cfs_dct.update({"cashfromoperations": cashop.text})
                except:
                    continue    
        
            for capex_term in capex_lst:
                capex_elem = version + capex_term
                try: 
                    for capex in root.iter(capex_elem):
                        if capex.get("contextRef") == date_code:
                            cfs_dct.update({"capex": capex.text})
                except:
                    continue        

            for acq_term in acq_lst:
                acq_elem = version + acq_term
                try: 
                    for acq in root.iter(acq_elem):
                        if acq.get("contextRef") == date_code:
                            cfs_dct.update({"acquisitionspend": acq.text})
                except:
                    continue 

            for cashinv_term in cashinv_lst:
                cashinv_elem = version + cashinv_term
                try: 
                    for cashinv in root.iter(cashinv_elem):
                        if cashinv.get("contextRef") == date_code:
                            cfs_dct.update({"cashfrominvesting": cashinv.text})                            
                except:
                    continue

            for debtissued_term in debtissued_lst:
                debtissued_elem = version + debtissued_term
                try: 
                    for debtissued in root.iter(debtissued_elem):
                        if debtissued.get("contextRef") == date_code:
                            cfs_dct.update({"debtissuance": debtissued.text})
                except:
                    continue

            for debtrepaid_term in debtrepaid_lst:
                debtrepaid_elem = version + debtrepaid_term
                try: 
                    for debtrepaid in root.iter(debtrepaid_elem):
                        if debtrepaid.get("contextRef") == date_code:
                            cfs_dct.update({"debtrepayment": debtrepaid.text})                            
                except:
                    continue

            for equityissued_term in equityissued_lst:
                equityissued_elem = version + equityissued_term
                try: 
                    for equityissued in root.iter(equityissued_elem):
                        if equityissued.get("contextRef") == date_code:
                            cfs_dct.update({"equityissuance": equityissued.text})                            
                except:
                    continue
            
            for equitybought_term in equitybought_lst:
                equitybought_elem = version + equitybought_term
                try: 
                    for equitybought in root.iter(equitybought_elem):
                        if equitybought.get("contextRef") == date_code:
                            cfs_dct.update({"sharebuyback": equitybought.text})                            
                except:
                    continue

            for cashfin_term in cashfin_lst:
                cashfin_elem = version + cashfin_term
                try: 
                    for cashfin in root.iter(cashfin_elem):
                        if cashfin.get("contextRef") == date_code:
                            cfs_dct.update({"cashfromfinancing": cashfin.text})                            
                except:
                    continue

        if len(cfs_dct) < 4:
            continue
        else:
#            period = context.find("{http://www.xbrl.org/2003/instance}period")
            start_date = period.find("{http://www.xbrl.org/2003/instance}startDate").text
            start_date_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
            end_date = period.find("{http://www.xbrl.org/2003/instance}endDate").text
            end_date_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
            delta = end_date_dt - start_date_dt
            days = delta.days
            print ("CFS No. of days:", days)
#            if days < 110:
#                new_quarter = "Q1" + report_period[0:4]
#            elif 165 < days < 195:
#                new_quarter = "Q2" + report_period[0:4]
#            elif 255 < days < 285:
#                new_quarter = "Q3" + report_period[0:4]
#            elif 355 < days < 370:
#                new_quarter = "FY" + report_period[0:4]
#            print ("CFS quarter", new_quarter)
#            
#            if quarter == new_quarter:
#                quarter = quarter
#                print ("Quarter code is same")
#            else:
#                quarter = new_quarter
#                is_dct.update({"quarter": quarter})
#                bs_dct.update({"quarter": quarter})
#                print ("Quarter code is different")
            
            new_dct = {"cfs_datecode": date_code, "report_period": report_period, "report_type": report_type, "start_date": start_date, "days_in_period": days, "quarter": quarter}
            cfs_dct.update(new_dct)
            break
        break
        
    master_dct.update({"Cash Flow Statement": cfs_dct})    
#    master_dct.update({"Cash Flow Statement": datacfs_lst})
    
    return master_dct, is_dct, bs_dct, cfs_dct

#fs = xbrl_scraper(urlname2 = "https://www.sec.gov/Archives/edgar/data/899051/000089905118000044/0000899051-18-000044-index.htm", report_type = "10-Q")    


def df_merge(df_is, df_bs, df_cfs):
    
    
    """
    The function df_merge makes sure the data in each df_is, df_bs and df_cfs dataframes are orderly
    and same format and compatible for pushing it to the database. It handles missing data, changes all data
    to one format and adds columns of zeroes when a column is not available for one company so that all
    companies have exactly same no. of columns
    """"

    ######### INCOME STATEMENT TTM #################
    dfis = df_is
    dfis = dfis.dropna(thresh = 6)

    try:
        dfis["report_period"] = dfis["report_period"].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d"))
        dfis = dfis.sort_values(by = "report_period")
    except:
        print ("Data not found, filling columns to zeroes")
        pass
    
    
    col_lst = list(dfis.columns)
    
    if "Diluted Shares O/S" in col_lst:
        dfis["Diluted Shares O/S"] = dfis["Diluted Shares O/S"].fillna(method = "ffill")
    
    dfis = dfis.fillna(0)
    
#    dfis = dfis.set_index("quarter")

    if "sales" in col_lst:
        dfis["sales"] = dfis["sales"].apply(lambda x: float(x))
    else:
        dfis["sales"] = [0 for i in range(len(dfis))]

    if "costofsales" in col_lst:
        dfis["costofsales"] = dfis["costofsales"].apply(lambda x: float(x))
    else:
        dfis["costofsales"] = [0 for i in range(len(dfis))]

    if "sellinggeneraladmin" in col_lst:
        dfis["sellinggeneraladmin"] = dfis["sellinggeneraladmin"].apply(lambda x: float(x))
    else:
        dfis["sellinggeneraladmin"] = [0 for i in range(len(dfis))]

    if "generaladmin" in col_lst:
        dfis["generaladmin"] = dfis["generaladmin"].apply(lambda x: float(x))
    else:
        dfis["generaladmin"] = [0 for i in range(len(dfis))]
        
    if "marketing" in col_lst:
        dfis["marketing"] = dfis["marketing"].apply(lambda x: float(x))
    else:
        dfis["marketing"] = [0 for i in range(len(dfis))]
        
    if "salary" in col_lst:
        dfis["salary"] = dfis["salary"].apply(lambda x: float(x))
    else:
        dfis["salary"] = [0 for i in range(len(dfis))]        

    if "researchdevelopment" in col_lst:
        dfis["researchdevelopment"] = dfis["researchdevelopment"].apply(lambda x: float(x))
    else:
        dfis["researchdevelopment"] = [0 for i in range(len(dfis))]        

    if "operatingexpense" in col_lst:
        dfis["operatingexpense"] = dfis["operatingexpense"].apply(lambda x: float(x))
    else:
        dfis["operatingexpense"] = [0 for i in range(len(dfis))]

    if "restructuringexpense" in col_lst:
        dfis["restructuringexpense"] = dfis["restructuringexpense"].apply(lambda x: float(x))
    else:
        dfis["restructuringexpense"] = [0 for i in range(len(dfis))]

    if "impairmentexpense" in col_lst:
        dfis["impairmentexpense"] = dfis["impairmentexpense"].apply(lambda x: float(x))
    else:
        dfis["impairmentexpense"] = [0 for i in range(len(dfis))]

    if "litigationexpense" in col_lst:
        dfis["litigationexpense"] = dfis["litigationexpense"].apply(lambda x: float(x))
    else:
        dfis["litigationexpense"] = [0 for i in range(len(dfis))]

    if "operatingincome" in col_lst:
        dfis["operatingincome"] = dfis["operatingincome"].apply(lambda x: float(x))
    else:
        dfis["operatingincome"] = [0 for i in range(len(dfis))]

    if "extinguishmentdebt" in col_lst:
        dfis["extinguishmentdebt"] = dfis["extinguishmentdebt"].apply(lambda x: float(x))
    else:
        dfis["extinguishmentdebt"] = [0 for i in range(len(dfis))]

    if "nonoperatingexpense" in col_lst:
        dfis["nonoperatingexpense"] = dfis["nonoperatingexpense"].apply(lambda x: float(x))
    else:
        dfis["nonoperatingexpense"] = [0 for i in range(len(dfis))]

    if "interestexpense" in col_lst:
        dfis["interestexpense"] = dfis["interestexpense"].apply(lambda x: float(x))
    else:
        dfis["interestexpense"] = [0 for i in range(len(dfis))]

    if "pretaxincome" in col_lst:
        dfis["pretaxincome"] = dfis["pretaxincome"].apply(lambda x: float(x))
    else:
        dfis["pretaxincome"] = [0 for i in range(len(dfis))]

    if "incometax" in col_lst:
        dfis["incometax"] = dfis["incometax"].apply(lambda x: float(x))
    else:
        dfis["incometax"] = [0 for i in range(len(dfis))]

    if "totalexpense" in col_lst:
        dfis["totalexpense"] = dfis["totalexpense"].apply(lambda x: float(x))
    else:
        dfis["totalexpense"] = [0 for i in range(len(dfis))]
    
    if "netincome" in col_lst:
        dfis["netincome"] = dfis["netincome"].apply(lambda x: float(x))
    else:
        dfis["netincome"] = [0 for i in range(len(dfis))]

    if "gaapdilutedeps" in col_lst:
        dfis["gaapdilutedeps"] = dfis["gaapdilutedeps"].apply(lambda x: float(x))
    else:
        dfis["gaapdilutedeps"] = [0 for i in range(len(dfis))]

    if "dilutedsharesos" in col_lst:
        dfis["dilutedsharesos"] = dfis["dilutedsharesos"].apply(lambda x: float(x))
    else:
        dfis["dilutedsharesos"] = [0 for i in range(len(dfis))]

    if "dps" in col_lst:
        dfis["dps"] = dfis["dps"].apply(lambda x: float(x))
    else:
        dfis["dps"] = [0 for i in range(len(dfis))]
    
    if "days_in_period" in col_lst:
        dfis["days_in_period"] = dfis["days_in_period"].apply(lambda x: float(x))
    else:
        dfis["days_in_period"] = [0 for i in range(len(dfis))]
        
        
    ######### CASH FLOW STATEMENT #################
    # dfcfs = dfs[2]
    dfcfs = df_cfs
    dfcfs = dfcfs.dropna(thresh = 5)

    try:
        dfcfs["report_period"] = dfcfs["report_period"].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d"))
        dfcfs = dfcfs.sort_values(by = "report_period")
    except:
        print ("Data not found, filling columns to zeroes")
        pass
    
    dfcfs = dfcfs.fillna(0)
    
#    dfcfs = dfcfs.set_index("quarter")
    
    col_lst = list(dfcfs.columns)
    
    
    if "da" in col_lst:
        dfcfs["da"] = dfcfs["da"].apply(lambda x: float(x))
    else:
        dfcfs["da"] = [0 for i in range(len(dfcfs))]
    
    if "sbcomp" in col_lst:
        dfcfs["sbcomp"] = dfcfs["sbcomp"].apply(lambda x: float(x))
    else:
        dfcfs["sbcomp"] = [0 for i in range(len(dfcfs))]
    
    if "cashfromoperations" in col_lst:
        dfcfs["cashfromoperations"] = dfcfs["cashfromoperations"].apply(lambda x: float(x))
    else:
        dfcfs["cashfromoperations"] = [0 for i in range(len(dfcfs))]
    
    if "capex" in col_lst:
        dfcfs["capex"] = dfcfs["capex"].apply(lambda x: float(x))
    else:
        dfcfs["capex"] = [0 for i in range(len(dfcfs))]
    
    if "acquisitionspend" in col_lst:
        dfcfs["acquisitionspend"] = dfcfs["acquisitionspend"].apply(lambda x: float(x))
    else:
        dfcfs["acquisitionspend"] = [0 for i in range(len(dfcfs))]
    
    if "cashfrominvesting" in col_lst:
        dfcfs["cashfrominvesting"] = dfcfs["cashfrominvesting"].apply(lambda x: float(x))
    else:
        dfcfs["cashfrominvesting"] = [0 for i in range(len(dfcfs))]
    
    if "debtissuance" in col_lst:
        dfcfs["debtissuance"] = dfcfs["debtissuance"].apply(lambda x: float(x))
    else:
        dfcfs["debtissuance"] = [0 for i in range(len(dfcfs))]
    
    if "debtrepayment" in col_lst:
        dfcfs["debtrepayment"] = dfcfs["debtrepayment"].apply(lambda x: float(x))
    else:
        dfcfs["debtrepayment"] = [0 for i in range(len(dfcfs))]
    
    if "equityissuance" in col_lst:
        dfcfs["equityissuance"] = dfcfs["equityissuance"].apply(lambda x: float(x))
    else:
        dfcfs["equityissuance"] = [0 for i in range(len(dfcfs))]
    
    if "sharebuyback" in col_lst:
        dfcfs["sharebuyback"] = dfcfs["sharebuyback"].apply(lambda x: float(x))
    else:
        dfcfs["sharebuyback"] = [0 for i in range(len(dfcfs))]
    
    if "cashfromfinancing" in col_lst:
        dfcfs["cashfromfinancing"] = dfcfs["cashfromfinancing"].apply(lambda x: float(x))
    else:
        dfcfs["cashfromfinancing"] = [0 for i in range(len(dfcfs))]


    ######### BALANCE SHEET #################

    dfbs = df_bs
    dfbs = dfbs.dropna(thresh = 5)
    
    try:
        dfbs["report_period"] = dfbs["report_period"].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d"))
        dfbs = dfbs.sort_values(by = "report_period")
    except:
        print ("Data not found, filling columns to zeroes")
        pass
        
    dfbs = dfbs.fillna(0)
    
    col_lst = list(dfbs.columns)
    
    if "cashandcashequivalents" in col_lst:
        dfbs["cashandcashequivalents"] = dfbs["cashandcashequivalents"].apply(lambda x: float(x))
    else:
        dfbs["cashandcashequivalents"] = [0 for i in range(len(dfbs))]
    
    if "accountsreceivable" in col_lst:
        dfbs["accountsreceivable"] = dfbs["accountsreceivable"].apply(lambda x: float(x))
    else:
        dfbs["accountsreceivable"] = [0 for i in range(len(dfbs))]
    
    if "inventory" in col_lst:
        dfbs["inventory"] = dfbs["inventory"].apply(lambda x: float(x))
    else:
        dfbs["inventory"] = [0 for i in range(len(dfbs))]
    
    if "currentassets" in col_lst:
        dfbs["currentassets"] = dfbs["currentassets"].apply(lambda x: float(x))
    else:
        dfbs["currentassets"] = [0 for i in range(len(dfbs))]
    
    if "propertyplantequipment" in col_lst:
        dfbs["propertyplantequipment"] = dfbs["propertyplantequipment"].apply(lambda x: float(x))
    else:
        dfbs["propertyplantequipment"] = [0 for i in range(len(dfbs))]
    
    if "goodwill" in col_lst:
        dfbs["goodwill"] = dfbs["goodwill"].apply(lambda x: float(x))
    else:
        dfbs["goodwill"] = [0 for i in range(len(dfbs))]
    
    if "assets" in col_lst:
        dfbs["assets"] = dfbs["assets"].apply(lambda x: float(x))
    else:
        dfbs["assets"] = [0 for i in range(len(dfbs))]
    
    if "shorttermdebt" in col_lst:
        dfbs["shorttermdebt"] = dfbs["shorttermdebt"].apply(lambda x: float(x))
    else:
        dfbs["shorttermdebt"] = [0 for i in range(len(dfbs))]
    
    if "accountspayable" in col_lst:
        dfbs["accountspayable"] = dfbs["accountspayable"].apply(lambda x: float(x))
    else:
        dfbs["accountspayable"] = [0 for i in range(len(dfbs))]
    
    if "currentliabilities" in col_lst:
        dfbs["currentliabilities"] = dfbs["currentliabilities"].apply(lambda x: float(x))
    else:
        dfbs["currentliabilities"] = [0 for i in range(len(dfbs))]
    
    if "longtermdebt" in col_lst:
        dfbs["longtermdebt"] = dfbs["longtermdebt"].apply(lambda x: float(x))
    else:
        dfbs["longtermdebt"] = [0 for i in range(len(dfbs))]
    
    if "additionalpic" in col_lst:
        dfbs["additionalpic"] = dfbs["additionalpic"].apply(lambda x: float(x))
    else:
        dfbs["additionalpic"] = [0 for i in range(len(dfbs))]    
    
    if "retainedearnings" in col_lst:
        dfbs["retainedearnings"] = dfbs["retainedearnings"].apply(lambda x: float(x))
    else:
        dfbs["retainedearnings"] = [0 for i in range(len(dfbs))]    
    
    if "ncinterest" in col_lst:
        dfbs["ncinterest"] = dfbs["ncinterest"].apply(lambda x: float(x))
    else:
        dfbs["ncinterest"] = [0 for i in range(len(dfbs))]    
    
    if "equity" in col_lst:
        dfbs["equity"] = dfbs["equity"].apply(lambda x: float(x))
    else:
        dfbs["equity"] = [0 for i in range(len(dfbs))]    
    
    if "liabilitiesequity" in col_lst:
        dfbs["liabilitiesequity"] = dfbs["liabilitiesequity"].apply(lambda x: float(x))
    else:
        dfbs["liabilitiesequity"] = [0 for i in range(len(dfbs))]    
    
        
    return dfis, dfbs, dfcfs


# df_ttm

def database(df_is, df_bs, df_cfs): 

    
    """
    The function pushes the completed dataframe of income statement, balance sheet and cash flow statement
    into the database
    """
    
    
    df_is.to_sql(name = "income_statement", con = engine, if_exists = "append")
    df_bs.to_sql(name = "balance_sheet", con = engine, if_exists = "append")
    df_cfs.to_sql(name = "cash_flow_statement", con = engine, if_exists = "append")
    
    print ("Entered into the Database")
    time.sleep(10)
    print ("Restarting.....\n\n\n\n")
#    df_ttm.to_sql(name = "financial_data", con = engine, if_exists = "append")
    return 0


def edgar_crawler():
    
    
    """
    The function edgar_crawler pulls all the functions together. It starts with calling the sp500_cik
    which contains ciks, tickers and ids (alternative forms are available for this, see my wiki scraper).
    It then creates an url from the base url which becomes the argument for the first function first_scraper.
    Then second_scraper is called in a loop which opens every 10-K and 10-Q one at a time and starts 
    scraping the data from each filing using the edgar_crawler function. Once the is_dct, bs_dct and cfs_dct
    is returned by the edgar_scraper, it is appended into the respective empty dictionaries. Then the 
    df_merge function is called to do the formating. Once formating is complete, df_is, df_bs and df_cfs 
    is pushed into the dataframe using the database function.  
    Note that it's currently structured particularly to append non existing data to the database i.e.
    new quarters or anuuals
    """
    
    
    count = 0
    metadata = sp500_cik()
    ciks = metadata[0]
    tickers = metadata[1]
    ids = metadata[2]
    
    url = "https://www.sec.gov/cgi-bin/browse-edgar"
    x = 0
    for cik in ciks[x:x+505]:
        
        # Get all the quarters currently in the database
        cur.execute("select report_period from income_statement where ticker = (%s)", (tickers[x],))
        data = cur.fetchall()
        date_lst = []
        for date in data:
            try:
                date = date[0].strftime("%Y-%m-%d")
                date_lst.append(date)
            except:
                continue
            
        print (date_lst)

        report_lst = ["10-Q", "10-K"]
#        report_lst = ["10-Q"]
        
        df_is = pd.DataFrame()
        df_bs = pd.DataFrame()
        df_cfs = pd.DataFrame()
        
        for report_type in report_lst:
            if report_type == "10-Q":
                count = 10
                
                params = {'action': 'getcompany', 'CIK': cik, 'type': report_type, 'dateb': '', 'owner': 'exclude', 'count': count}
                url_parts = list(urllib.parse.urlparse(url))
                query = dict(urllib.parse.parse_qsl(url_parts[4]))
                query.update(params)
                url_parts[4] = urllib.parse.urlencode(query)
                urlname1 = urllib.parse.urlunparse(url_parts)
                page1 = first_scraper(urlname1)
                report_links = page1[1]
#                print (report_links)
                
                for urlname2 in report_links[0:4]:
#                    print (urlname2)
                    try:                        
                        report_period = second_scraper(urlname2)[2]
#                        print (report_period)
                    except:
#                        print ("Could not find the 10-Q xml file")
                        continue

                    # Check to see if the last 10-Q already exists in the database  
                    if report_period in date_lst:
                        print ("{0} already exists in database, 10-Q 1st check".format(report_period))
                        continue
                    else:
                        print ("{0} not in database, 10-Q 1st check".format(report_period))
                        pass

                    fs = xbrl_scraper(urlname2, report_type)
                    try:
                        is_dct = fs[1]
                        df_is = df_is.append(is_dct, ignore_index = True)
                    
                        bs_dct = fs[2]
                        df_bs = df_bs.append(bs_dct, ignore_index = True)
            
                        cfs_dct = fs[3]
                        df_cfs = df_cfs.append(cfs_dct, ignore_index = True)
                        
                        report_period = fs[4]
                        
                        if report_period in date_lst:
                            df_is = df_is.set_index("report_period")
                            df_is = df_is.drop(index = report_period)
                            df_is = df_is.reset_index()

                            df_bs = df_bs.set_index("report_period")
                            df_bs = df_bs.drop(index = report_period)
                            df_bs = df_bs.reset_index()
                            
                            df_cfs = df_cfs.set_index("report_period")
                            df_cfs = df_cfs.drop(index = report_period)
                            df_cfs = df_cfs.reset_index()                            
                            print ("{0} already exists in database, 10-Q 2nd check".format(report_period))
                            continue
                        else:
                            print ("{0} not in database, 10-Q 2nd check".format(report_period))
                            pass
                
                    except:
                        continue

            elif report_type == "10-K":
                count = 10
                
                params = {'action': 'getcompany', 'CIK': cik, 'type': report_type, 'dateb': '', 'owner': 'exclude', 'count': count}
                url_parts = list(urllib.parse.urlparse(url))
                query = dict(urllib.parse.parse_qsl(url_parts[4]))
                query.update(params)
                url_parts[4] = urllib.parse.urlencode(query)
                urlname1 = urllib.parse.urlunparse(url_parts)
                page1 = first_scraper(urlname1)
                report_links = page1[1]        
        
                for urlname2 in report_links[0:2]:
                    try:
                        report_period = second_scraper(urlname2)[2]
                        print (report_period)
                    except:
                        print ("Could not find the 10-K xml file")
                        continue
                    
                    # Check to see if the last 10-Q already exists in the database  
                    if report_period in date_lst:
                        print ("{0} already exists in database, 10-K 1st check".format(report_period))
                        continue
                    else:
                        print ("{0} not in database, 10-K 1st check".format(report_period))
                        pass                    
                    
                    fs = xbrl_scraper(urlname2, report_type)
                    try:
                        is_dct = fs[1]
                        df_is = df_is.append(is_dct, ignore_index = True)
                    
                        bs_dct = fs[2]
                        df_bs = df_bs.append(bs_dct, ignore_index = True)
            
                        cfs_dct = fs[3]
                        df_cfs = df_cfs.append(cfs_dct, ignore_index = True)
                        
                        report_period = fs[4]

                        if report_period in date_lst:
                            df_is = df_is.set_index("report_period")
                            df_is = df_is.drop(index = report_period)
                            df_is = df_is.reset_index()

                            df_bs = df_bs.set_index("report_period")
                            df_bs = df_bs.drop(index = report_period)
                            df_bs = df_bs.reset_index()
                            
                            df_cfs = df_cfs.set_index("report_period")
                            df_cfs = df_cfs.drop(index = report_period)
                            df_cfs = df_cfs.reset_index()                            
                            print ("{0} already exists in database, 10-K 2nd check".format(report_period))
                            continue
                        else:
                            print ("{0} not in database, 10-K 2nd check".format(report_period))

                    except:
                        continue
        
        df_is["cik"] = [cik for i in range (len(df_is))]
        df_is["ticker"] = [tickers[x] for i in range(len(df_is))]
        df_is["symbol_id"] = [ids[x] for i in range(len(df_is))]
#        df_is = df_is.dropna(thresh = 5)
        
        df_bs["cik"] = [cik for i in range (len(df_bs))]
        df_bs["ticker"] = [tickers[x] for i in range(len(df_bs))]
        df_bs["symbol_id"] = [ids[x] for i in range(len(df_bs))]
#        df_bs = df_bs.dropna(thresh = 5)
        
        df_cfs["cik"] = [cik for i in range (len(df_cfs))]
        df_cfs["ticker"] = [tickers[x] for i in range(len(df_cfs))]
        df_cfs["symbol_id"] = [ids[x] for i in range(len(df_cfs))]
#        df_cfs = df_cfs.dropna(thresh = 5)
        
#        df_ttm = df_merge(df_is, df_bs, df_cfs)
        
        df_fs = df_merge(df_is, df_bs, df_cfs)
        
        df_is = df_fs[0]
        df_bs = df_fs[1]
        df_cfs = df_fs[2]
        
        database(df_is, df_bs, df_cfs)
        
#        print ("symbol_id:", x)
        x += 1
                
    return df_is, df_bs, df_cfs


dfs = edgar_crawler()


## Simple database queries for the financial database
# select * from income_statement join symbol on income_statement.symbol_id=symbol.id where income_statement.ticker = "KMB"
## Alter column name
#cur.execute("alter table [table_name] change [old_column_name] [new_column_name] [data_type]")
