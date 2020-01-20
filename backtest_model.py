df["book_to_price_score"] = df["book_to_price_zscore"]*0.025
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 23 11:43:49 2019

@author: robert.curriedf["roic_score"] = df["roic_zscore"]*0.075

"""

import mysql.connector as mdb
import pandas as pd
import datetime
import time

import blpapi
import pdblp
import math
import numpy as np
from functools import reduce
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import pymysql

from cvxopt import blas, solvers, matrix

con = pdblp.BCon(debug = False, port = 8194, timeout = 50000)
con.start()

conn = mdb.connect(host = "localhost", user = "root", passwd = "delta1")
cur = conn.cursor()

cur.execute("USE msci_world")
engine = create_engine("mysql+pymysql://root: delta1@localhost: 3306/msci_world")

cd T:\Razin.Hussain\low_vol_global_equity

#query = '''
#select * from quarterly where price_date = "2004-12-31 00:00:00"
#'''
#df = pd.read_sql_query(query, engine)
#
#zscore = lambda x: (x - x.mean())/x.std()
#
#df_transform = df.groupby("sector")["1yr_std", "3yr_std", "5yr_std", "2yr_adj_beta", "fcf_yield", "book_to_price", "dividend_yield", "roe", "roic", "net_debt_to_ebitda"].transform(zscore)
#df_transform = df_transform.fillna(0)
#
#for col in df_transform.columns:
#    df_transform = df_transform.rename(columns = {col: col + "_" + "zscore"})
#
#df = pd.concat([df, df_transform], axis = 1)
#df["1yr_std_score"] = df["1yr_std_zscore"]*0.15
#df["3yr_std_score"] = df["3yr_std_zscore"]*0.25
#df["5yr_std_score"] = df["5yr_std_zscore"]*0.15
#df["2yr_adj_beta_score"] = df["2yr_adj_beta_zscore"]*0.15
#df["fcf_yield_score"] = df["fcf_yield_zscore"]*0.10
#df["book_to_price_score"] = df["book_to_price_zscore"]*0.025
#df["dividend_yield_score"] = df["dividend_yield_zscore"]*0.025
#df["roe_score"] = df["roe_zscore"]*0.025
#df["roic_score"] = df["roic_zscore"]*0.075
#df["net_debt_to_ebitda_score"] = df["net_debt_to_ebitda_zscore"]*0.05
#
#df["score"] = -df["1yr_std_score"] - df["3yr_std_score"] - df["5yr_std_score"] - df["2yr_adj_beta_score"] + df["fcf_yield_score"] + df["book_to_price_score"] + df["dividend_yield_score"] + df["roe_score"] + df["roic_score"] - df["net_debt_to_ebitda_score"]
#df["rank"] = df["score"].rank(ascending = False)
#df = df.sort_values(by = ["score"], ascending = False)
#
#p = 0.065
#df = (df.groupby("sector", group_keys = False).apply(lambda x: x.nlargest(int(len(x) * p), "score")))
#
#df.to_csv("test53.csv")

date = "2005-03-31 00:00:00"
query = '''
select * from quarterly where price_date = %s
'''
df = pd.read_sql_query(query, engine, params = (date,))
df = df.dropna(subset = ["sector"])

df.to_csv("test65.csv")


def z_score(rebalancing_date):
    engine = create_engine("mysql+pymysql://root: delta1@localhost: 3306/msci_world")
    query = '''
    select * from quarterly where price_date = %s
    '''
    df = pd.read_sql_query(query, engine, params = (rebalancing_date,))
    
#    key = lambda x: x.sector
    zscore = lambda x: (x - x.mean())/x.std()
    
    # Remove any rows where 1yr and 3yr std is not present 
#    df = df.dropna(subset = ["1yr_std", "3yr_std"]) 
    
    df_transform = df.groupby("sector")["1yr_std", "3yr_std", "5yr_std", "2yr_adj_beta", "fcf_yield", "book_to_price", "dividend_yield", "roe", "roic", "net_debt_to_ebitda"].transform(zscore)
    df_transform = df_transform.fillna(0)
    
    # Change the column names  
    for col in df_transform.columns:
        df_transform = df_transform.rename(columns = {col: col + "_" + "zscore"})
    
    df = pd.concat([df, df_transform], axis = 1)
    
    # Calculate the scores 

#    factor_weights = {"1yr_std_weight": 0.30, "3yr_std_weight": 0.15, "5yr_std_weight": 0.10, "2yr_adj_beta_weight": 0.15, "fcf_yield_weight": 0.10, "book_to_price_weight": 0.025, "dividend_yield_weight": 0.025, "roe_weight": 0.05, "roic_weight": 0.05, "net_debt_to_ebitda_weight": 0.05}
#    df_weight = pd.DataFrame(factor_weights)

    df["1yr_std_score"] = df["1yr_std_zscore"]*0.15
    df["3yr_std_score"] = df["3yr_std_zscore"]*0.25
    df["5yr_std_score"] = df["5yr_std_zscore"]*0.15
    df["2yr_adj_beta_score"] = df["2yr_adj_beta_zscore"]*0.15
    df["fcf_yield_score"] = df["fcf_yield_zscore"]*0.10
    df["book_to_price_score"] = df["book_to_price_zscore"]*0.025
    df["dividend_yield_score"] = df["dividend_yield_zscore"]*0.025
    df["roe_score"] = df["roe_zscore"]*0.025
    df["roic_score"] = df["roic_zscore"]*0.075
    df["net_debt_to_ebitda_score"] = df["net_debt_to_ebitda_zscore"]*0.05

    df["score"] = -df["1yr_std_score"] - df["3yr_std_score"] - df["5yr_std_score"] - df["2yr_adj_beta_score"] + df["fcf_yield_score"] + df["book_to_price_score"] + df["dividend_yield_score"] + df["roe_score"] + df["roic_score"] - df["net_debt_to_ebitda_score"]
    # Rank companies based on scores
    df["rank"] = df["score"].rank(ascending = False)

    df = df.sort_values(by = ["score"], ascending = False)
#    
    return df

# Pick top 7% company from every sector 
def basket(rebalancing_date):
    df = z_score(rebalancing_date)
    
    # Sorting within groups based on column "score"
    p = 0.075

    df = (df.groupby("sector", group_keys = False).apply(lambda x: x.nlargest(int(len(x) * p), "score")))
    
    return df

#
#df = basket("2013-09-30 00:00:00")
#df.to_csv("test79.csv")

# Start of the optimization module
# Fetching price data from bb to calculate annual returns for companies and covariance matrix
def data_collector(rebalancing_date, period_3yr, cutoff_date):
    quant_basket = basket(rebalancing_date)
    quant_basket = quant_basket.sort_values(by = ["country", "rank"])
    ticker_lst = [ticker for ticker in quant_basket["bb_ticker"]]

    # Create a df list to hold all company level df
#    df_price_lst = []
    df_dailyreturn_lst = []
    # Scrap close_price for each company from msci_world database
      
    for bb_ticker in ticker_lst:
        df = con.bdh(bb_ticker, "PX_LAST", period_3yr, cutoff_date)
        df.columns = df.columns.droplevel("field")
        
        try:
            df.columns = [bb_ticker]
        except:
            quant_basket = quant_basket.set_index("bb_ticker")
            quant_basket = quant_basket.drop([bb_ticker])
            quant_basket = quant_basket.reset_index()
            continue
        
##        df_price = df.resample(rebalancing_term).last()
##        df_price = df_price.reset_index()
##        df_price_lst.append(df_price)
##        
        df_dailyreturn = df.pct_change(1)
        df_dailyreturn = df_dailyreturn.reset_index()
        df_dailyreturn_lst.append(df_dailyreturn)
    
    # Concatenate the list of price dfs
#    df_price = reduce(lambda left, right: pd.merge(left, right, how = "inner", on = "date"), df_price_lst)
    
    # Concatenate the list of daily return dfs
    df_dailyreturn = reduce(lambda left, right: pd.merge(left, right, how = "inner", on = "date"), df_dailyreturn_lst)
    df_dailyreturn = df_dailyreturn.fillna(0)
    df_dailyreturn = df_dailyreturn.set_index("date")
    
#    df_dailyreturn = df_dailyreturn.iloc[1:]
    
    # Calculating covariance matrix and annualized return matrix
    cov_matrix = df_dailyreturn.cov()
    annual_return = df_dailyreturn.mean()*252
    return_matrix = annual_return.as_matrix()
    
    return df_dailyreturn, cov_matrix, return_matrix, ticker_lst, quant_basket


#
#rebalancing_date = "2005-03-31 00:00:00"
#period_3yr = "20020401"
#cutoff_date = "20050331"
##
#x = data_collector(rebalancing_date, period_3yr, cutoff_date)


def portfolio_constraints(number_of_stocks, sector_lst, companycount_by_country_lst):
    n = number_of_stocks
    
    # Adding upper and lower company constraint functions (see documentation for the formulation of the optimization problem)
    G_company_lower = matrix(0.0, (n,n))
    G_company_lower[::n+1] = -1.0
    G_company_upper = matrix(0.0, (n,n))
    G_company_upper[::n+1] = 1.0
    
    G = np.append(G_company_lower, G_company_upper, axis = 0)

    h_company_lower = matrix(-0.006, (n,1)) # Minimum weight that can be invested in a company 
    h_company_upper = matrix(0.035, (n,1))  # Maximum weight that can be invested in a company

    h = np.append(h_company_lower, h_company_upper, axis = 0)
    
    # Adding sector limits (see documentation)
    
    # Append 1 by n matrix for every sector
    lst = [i for i, n in enumerate(sector_lst) if n == "Communication"]
    x = np.zeros((n))
    i = np.array(lst)
    x[i] = 1.0
    x = x.reshape((1,n))
    G = np.append(G, x, axis = 0)
    
    lst = [i for i, n in enumerate(sector_lst) if n == "Consumer Discretionary"]
    x = np.zeros((n))
    i = np.array(lst)
    x[i] = 1.0
    x = x.reshape((1,n))
    G = np.append(G, x, axis = 0)

    lst = [i for i, n in enumerate(sector_lst) if n == "Consumer Staples"]
    x = np.zeros((n))
    i = np.array(lst)
    x[i] = 1.0
    x = x.reshape((1,n))
    G = np.append(G, x, axis = 0)
    
    lst = [i for i, n in enumerate(sector_lst) if n == "Energy"]
    x = np.zeros((n))
    i = np.array(lst)
    x[i] = 1.0
    x = x.reshape((1,n))
    G = np.append(G, x, axis = 0)

    lst = [i for i, n in enumerate(sector_lst) if n == "Financials"]
    x = np.zeros((n))
    i = np.array(lst)
    x[i] = 1.0
    x = x.reshape((1,n))
    G = np.append(G, x, axis = 0)
    
    lst = [i for i, n in enumerate(sector_lst) if n == "Health Care"]
    x = np.zeros((n))
    i = np.array(lst)
    x[i] = 1.0
    x = x.reshape((1,n))
    G = np.append(G, x, axis = 0)

    lst = [i for i, n in enumerate(sector_lst) if n == "Industrials"]
    x = np.zeros((n))
    i = np.array(lst)
    x[i] = 1.0
    x = x.reshape((1,n))
    G = np.append(G, x, axis = 0)
    
    lst = [i for i, n in enumerate(sector_lst) if n == "Information Technology"]
    x = np.zeros((n))
    i = np.array(lst)
    x[i] = 1.0
    x = x.reshape((1,n))
    G = np.append(G, x, axis = 0)

    lst = [i for i, n in enumerate(sector_lst) if n == "Materials"]
    x = np.zeros((n))
    i = np.array(lst)
    x[i] = 1.0
    x = x.reshape((1,n))
    G = np.append(G, x, axis = 0)
    
    lst = [i for i, n in enumerate(sector_lst) if n == "Real Estate"]
    x = np.zeros((n))
    i = np.array(lst)
    x[i] = 1.0
    x = x.reshape((1,n))
    G = np.append(G, x, axis = 0)
    
    lst = [i for i, n in enumerate(sector_lst) if n == "Utilities"]
    x = np.zeros((n))
    i = np.array(lst)
    x[i] = 1.0
    x = x.reshape((1,n))
    G = np.append(G, x, axis = 0)

    # The sector weights are in order of Communication, Consumer Discretionary, Consumer Staples, Energy, Financials, Healthcare, Industrials, IT, Materials, Real Estate, Utilities
#    h_sector_cap = matrix([-0.044, 0.164, -0.0557, 0.1857, -0.0443, 0.1643, -0.0208, 0.1408, -0.1224, 0.2424, -0.0903, 0.2103, -0.071, 0.191, -0.1092, 0.2292, -0.0161, 0.1261, -0.0028, 0.1128, -0.0035, 0.1135])

    msci_sector_weight = matrix([0.0840, 0.1057, 0.0843, 0.0608, 0.1624, 0.1303, 0.1110, 0.1492, 0.0461, 0.0328, 0.0335])
    sector_delta = 0.10
    h_sector_cap = msci_sector_weight + sector_delta
    

    h = np.append(h, h_sector_cap, axis = 0)
    
    # Adding country limits
    country_total = 0
    for country_count in companycount_by_country_lst:
        previous_country_total = country_total
        country_total = previous_country_total + country_count

#        G_country_lower = matrix(0.0, (1,n))
#        G_country_lower[:, previous_country_total:country_total] = -1.0

        G_country_upper = matrix(0.0, (1,n))
        G_country_upper[:, previous_country_total:country_total] = 1.0
        
        G = np.append(G, G_country_upper, axis = 0)
    # Canada original - 3.48% and Australia - 2.4%
    # The country weights are in order of Australia, Austria, Belgium, Canada, Denmark, Finland, France, Germany, Hong Kong, Ireland, Israel, Italy, Japan, Netherlands, New Zealnd,  Norway, Portugal, Singapore, Spain, Sweden, Switzerland, UK and US
    msci_country_weight = matrix([0.010, 0.0036, 0.0036, 0.015, 0.0036, 0.0036, 0.0376, 0.0297, 0.0132, 0.0036, 0.0036, 0.0036, 0.0851, 0.0116, 0.0036, 0.0036, 0.0036, 0.0036, 0.0107, 0.0036, 0.0300, 0.0582, 0.6193]) 
    country_delta = 0.10   # Maximum overweight above the index country weights
    h_country_upper = msci_country_weight + country_delta

    h = np.append(h, h_country_upper, axis = 0)
    
    G = matrix(G)
    h = matrix(h)

    # All weights needs to add to one
    A = matrix(1.0, (1, n))
    b = matrix(1.0)
    
    return G, h, A, b


def cash_balance():
    cash_weight = {"bb_ticker": "**CASH**", "optimal_weight": 0.010, "index_weight": 0.010, 
                   "equal_weight": 0.010, "company_name": None, "sector": None, "country": "Canada"}
    cash_weight = pd.DataFrame(cash_weight, index = [0])
    
    return cash_weight


# Module to search through the efficient frontier in 20bps increments
def std_search(required_std, df):
    count = 0
    for i in df["minimum_volatility"]:
        count += 1
        j = df["minimum_volatility"][count]
        if (((i - required_std > 0.0) and (j - required_std > 0.0)) or ((i - required_std < 0.0) and (j - required_std < 0.0))):
            if count < (len(df) - 1):
                continue
            elif count == (len(df) - 1):
                required_std += 0.002
#                print ("Adding 0.002")
                return std_search(required_std, df)
        else:
            right = i - required_std
            left = required_std - j
            if right < left:
                print (i)
                df = df[df["minimum_volatility"] == i]
                return df
            elif left < right:
                print (j)
                df = df[df["minimum_volatility"] == j]
                return df


#required_std = 0.07
#df = pd.DataFrame()
#df["minimum_volatility"] = [0.154584544, 0.14877654634, 0.145698554, 0.136876744, 0.1274874864, 0.1087464444, 0.0941546544, 0.0865341355, 0.0795455345]
#df["ABC"] = [0.05, 0.05, 0.07, 0.009, 0.055898, 0.01544, 0.025874, 0.254544, 0.0545874]
#df["DEF"] = [0.05454, 0.12545, 0.02121, 0.02588, 0.23589, 0.036894, 0.0123578, 0.00598421, 0.01258955]
#
#df_copy = std_search(required_std, df)
#df_copy


def optimal_portfolio(rebalancing_date, period_3yr, cutoff_date):
    data = data_collector(rebalancing_date, period_3yr, cutoff_date)
    df1 = data[0]
    number_of_stocks = len(df1.columns)
    
    # Get quarter end price
#    allocation_price = data[5]
#    ticker_lst = [ticker for ticker in df1.columns]
    ticker_lst = data[3]
    
    df2 = data[4]
    df2 = df2.rename(columns = {"weight": "index_weight", "name": "company_name"})
    sector_lst = df2["sector"].tolist()
#    print (companycount_by_sector_lst)

    companycount_by_country = df2.groupby("country").count()
    companycount_by_country = companycount_by_country.reset_index()
    portfolio_country_lst = companycount_by_country["country"].tolist()
    
    msci_country_lst = ["Australia", "Austria", "Belgium", "Canada", "Denmark", "Finland", "France", "Germany", "Hong Kong", "Ireland", "Israel", "Italy", "Japan", "Netherlands", "New Zealand", "Norway", "Portugal", "Singapore", "Spain", "Sweden", "Switzerland", "United Kingdom", "United States"]

    count = 0
    companycount_by_country_lst = []
    for country in msci_country_lst:
        if country in portfolio_country_lst:
            companycount_by_country_lst.append(companycount_by_country["bb_ticker"][count])
            count = count + 1
        else:
            companycount_by_country_lst.append(0)
    
#    print (companycount_by_country_lst)
    
    constraint_matrices = portfolio_constraints(number_of_stocks, sector_lst, companycount_by_country_lst)
    G = constraint_matrices[0]
    h = constraint_matrices[1]
    A = constraint_matrices[2]
    b = constraint_matrices[3]
    
    # Adding covariance and return matrix for the objective function
    Q = data[1]  # Covariance matrix (3 year average annualized)
    Q = np.matrix(Q)
    Q = matrix(Q)
    
    p = data[2]   # Returns matrix   (3 year average annualized) 
    p = matrix(p)
    
    # Adding a list called rhos which represents proportion of var-covar and return to optimize
    N = 100
    rhos = [100**(5 * t/N - 1.0) for t in range(N)] 
    
    # Calculating the efficient frontier weights with qp by calling the solvers function in cvxopt
    portfolios = [solvers.qp(rho*Q, -p, G, h, A, b)["x"] for rho in rhos]
    
    # Calculating return and std for each efficient frontier portfolio
    returns = [blas.dot(p, x) for x in portfolios]
    std = [(np.sqrt(blas.dot(x, Q*x)))*math.sqrt(252) for x in portfolios]
    
    df = pd.DataFrame()
    df["optimal_returns"] = returns
    df["minimum_volatility"] = std
    
    for counter, symbol in enumerate(ticker_lst):
        df[symbol] = [weight[counter] for weight in portfolios]

    # Closest weight that gives 7.0% standard deviation    
    required_std = 0.0680
    df_copy = std_search(required_std, df)

    optimal_moments = df_copy.iloc[:, 0:2]
    optimal_weights = df_copy.iloc[:, 2:]
    
    # Convert the dataframe to a series to convert the column names to rows 
    optimal_weights = optimal_weights.T.squeeze()
    
    final_df = pd.DataFrame()
    final_df["optimal_weight"] = optimal_weights
    final_df = final_df.reset_index()
    final_df = final_df.rename(columns = {"index": "bb_ticker"})
    
    df2_retain = df2[["index_weight", "bb_ticker", "company_name", "sector", "country"]]
#    df_retain = df_retain.set_index("bb_ticker")

    final_df = pd.merge(final_df, df2_retain, how = "inner", on = "bb_ticker")
    final_df["index_weight"] = final_df["index_weight"].apply(lambda x: x/final_df["index_weight"].sum())
    
    final_df["equal_weight"] = [0.990/len(final_df) for i in range (len(final_df))]
#    columns_to_keep = ["bb_ticker", "optimal_weight", "index_weight", "equal_weight", "company_name", "sector", "country"]
#    final_df = final_df[columns_to_keep]
    
    # Imposing upper limit for company weight
    count = 0
    for weight in final_df["index_weight"]:
        upper_limit = 0.040
        if weight > upper_limit:
            final_df.iloc[count:count+1, 2:3] = upper_limit
            increment = (weight - upper_limit)/len(final_df)
            final_df["index_weight"] = final_df["index_weight"] + increment
        count = count + 1
    
    # Rescaling equity weights to equal 98.5% total equity weight and 1.5% cash balane
    final_df["optimal_weight"] = final_df["optimal_weight"].apply(lambda x: (x/final_df["optimal_weight"].sum())*0.990)
    final_df["index_weight"] = final_df["index_weight"].apply(lambda x: (x/final_df["index_weight"].sum())*0.990)    
    
    # Calling the cash function
    df_cash = cash_balance()
    final_df = pd.concat([df_cash, final_df], axis = 0, ignore_index = True)

    columns_to_keep = ["bb_ticker", "optimal_weight", "index_weight", "equal_weight", "company_name", "sector", "country"]
    final_df = final_df[columns_to_keep]
    
    try:
        final_df.to_csv("backtest_results\port_{0}.csv".format(cutoff_date))
    except:
        print ("The quarter already exists in Archive")

    # df2 is the quant basket - basket of securities with scores and score attributes
    # df contains all the optimal portfolios in the efficient frontier
    # final_df contains securities to be invested in along with the weights
    # Return and std of the optimal portfolio we are investing in    
    return df2, df, final_df, optimal_moments, ticker_lst


x = optimal_portfolio(rebalancing_date, period_3yr, cutoff_date)


def backtest(rebalancing_date, period_3yr, cutoff_date, quarter_end):
    data = optimal_portfolio(rebalancing_date, period_3yr, cutoff_date)
    df_weight = data[2]
    df_weight = df_weight[["bb_ticker", "optimal_weight"]]
    
    # Removing cash from the dataframe
    df_weight = df_weight.iloc[1:]
    
    ticker_lst = [ticker for ticker in df_weight["bb_ticker"]]
    df_returnlst = []
    df_divlst = []
    for ticker in ticker_lst:
        try:
            df = con.bdh(ticker, ["PX_LAST", "EQY_DVD_YLD_12M"], cutoff_date, quarter_end)
        except:
            continue
        df.columns = df.columns.droplevel("field")
        df = df.reset_index()
        if len(df.columns) == 2:
#            div_ticker = ticker.replace(ticker[:], ticker[0:6])
            df[ticker.replace(ticker[:], ticker[0:6])] = [0.0 for i in range (len(df))]
        
        df_price = df.iloc[:, [0, 1]]
        df_price = df_price.set_index("date")
        
        try:
            df_price.columns = [ticker]
        except:
            df_weight = df_weight.set_index("bb_ticker")
            df_weight = df_weight.drop([ticker])
            df_weight = df_weight.reset_index()
            continue
        
        df_price = df_price.pct_change(1)
        df_price = df_price.iloc[1:]
        df_price = df_price.reset_index()
        df_returnlst.append(df_price)
        
        df_div = df.iloc[:, [0, 2]]
        df_div = df_div.set_index("date")
        df_div = df_div.iloc[1:]
        df_div = df_div.apply(lambda x: (x/100)/252)
        df_div = df_div.reset_index()
        df_divlst.append(df_div)
    
    df_dailyreturn = reduce(lambda left, right: pd.merge(left, right, how = "outer", on = "date"), df_returnlst)
    df_dailydiv = reduce(lambda left, right: pd.merge(left, right, how = "outer", on = "date"), df_divlst)

    df_dailyreturn = df_dailyreturn.fillna(0)
    df_dailyreturn = df_dailyreturn.sort_values(by = "date")
    
    df_dailydiv = df_dailydiv.fillna(0)
    df_dailydiv = df_dailydiv.sort_values(by = "date")
    
    df_dailyreturn = df_dailyreturn.set_index("date")
    return_matrix = df_dailyreturn.as_matrix()
    
    df_dailydiv = df_dailydiv.set_index("date")
    div_matrix = df_dailydiv.as_matrix()

    df_weight = df_weight.set_index("bb_ticker")
    weight_matrix = df_weight.as_matrix()
    
    df_dailyreturn = df_dailyreturn.reset_index()
    df_dailydiv = df_dailydiv.reset_index()
    
    capital_gain = np.dot(return_matrix, weight_matrix)
    div_yield = np.dot(div_matrix, weight_matrix)
    
    df_portreturn1 = pd.DataFrame()
    df_portreturn1["date"] = df_dailyreturn["date"]
    df_portreturn1["port_capitalgain"] = capital_gain
    df_portreturn2 = pd.DataFrame()
    df_portreturn2["date"] = df_dailydiv["date"]
    df_portreturn2["port_divyield"] = div_yield
    df_portreturn = pd.merge(df_portreturn1, df_portreturn2, how = "outer", on = "date")
    df_portreturn = df_portreturn.sort_values(by = "date")
    df_portreturn = df_portreturn.fillna(0)
    
    df_portreturn["port_totalreturn"] = df_portreturn["port_capitalgain"] + df_portreturn["port_divyield"]

#    df_portreturn["div_yield"] = div_yield
#    df_portreturn["port_totalreturn"] = df_portreturn["port_capitalgain"] + df_portreturn["div_yield"]
    
#    df = pd.DataFrame()
#    df = df.append(df_portreturn, ignore_index = True)
    
    return df_portreturn



## Q2 2018 - Working
rebalancing_date = "2018-06-30 00:00:00"
period_3yr = "20150701"
cutoff_date = "20180630"
quarter_end = "20180930"

## Q3 2018 - Working
rebalancing_date = "2018-09-30 00:00:00"
period_3yr = "20151001"
cutoff_date = "20180930"
quarter_end = "20181231"

## Q3 2013
rebalancing_date = "2013-09-30 00:00:00"
period_3yr = "20101001"
cutoff_date = "20130930"
quarter_end = "20131231"

## Q4 2012
rebalancing_date = "2012-12-31 00:00:00"
period_3yr = "20100101"
cutoff_date = "20121231"
quarter_end = "20130331"


## Q4 2012
rebalancing_date = '2005-12-31 00:00:00'
period_3yr = "20030101"
cutoff_date = "20051231"
quarter_end = "20060331"

## Q4 2018
rebalancing_date = '2018-12-31 00:00:00'
period_3yr = "20160101"
cutoff_date = "20181231"





test = backtest(rebalancing_date, period_3yr, cutoff_date, quarter_end)
test







































#############################################################################




# x = backtest('2016-03-31 00:00:00', "20130401", "20160331", "20160630")


rebalancing_date_lst = ['2004-12-31 00:00:00', '2005-03-31 00:00:00', '2005-06-30 00:00:00', '2005-09-30 00:00:00', 
                        '2005-12-31 00:00:00', '2006-03-31 00:00:00', '2006-06-30 00:00:00', '2006-09-30 00:00:00', 
                        '2006-12-31 00:00:00', '2007-03-31 00:00:00', '2007-06-30 00:00:00', '2007-09-30 00:00:00', 
                        '2007-12-31 00:00:00', '2008-03-31 00:00:00', '2008-06-30 00:00:00', '2008-09-30 00:00:00', 
                        '2008-12-31 00:00:00', '2009-03-31 00:00:00', '2009-06-30 00:00:00', '2009-09-30 00:00:00', 
                        '2009-12-31 00:00:00', '2010-03-31 00:00:00', '2010-06-30 00:00:00', '2010-09-30 00:00:00', 
                        '2010-12-31 00:00:00', '2011-03-31 00:00:00', '2011-06-30 00:00:00', '2011-09-30 00:00:00', 
                        '2011-12-31 00:00:00', '2012-03-31 00:00:00', '2012-06-30 00:00:00', '2012-09-30 00:00:00', 
                        '2012-12-31 00:00:00', '2013-03-31 00:00:00', '2013-06-30 00:00:00', '2013-09-30 00:00:00', 
                        '2013-12-31 00:00:00', '2014-03-31 00:00:00', '2014-06-30 00:00:00', '2014-09-30 00:00:00', 
                        '2014-12-31 00:00:00', '2015-03-31 00:00:00', '2015-06-30 00:00:00', '2015-09-30 00:00:00', 
                        '2015-12-31 00:00:00', '2016-03-31 00:00:00', '2016-06-30 00:00:00', '2016-09-30 00:00:00', 
                        '2016-12-31 00:00:00', '2017-03-31 00:00:00', '2017-06-30 00:00:00', '2017-09-30 00:00:00', 
                        '2017-12-31 00:00:00', '2018-03-31 00:00:00', '2018-06-30 00:00:00', '2018-09-30 00:00:00',
                        '2018-12-31 00:00:00']
#
# len(rebalancing_date_lst)
#
period_3yr_lst = ["20020101", "20020401", "20020701", "20021001",
                  "20030101", "20030401", "20030701", "20031001",
                  "20040101", "20040401", "20040701", "20041001",
                  "20050101", "20050401", "20050701", "20051001",
                  "20060101", "20060401", "20060701", "20061001",
                  "20070101", "20070401", "20070701", "20071001",
                  "20080101", "20080401", "20080701", "20081001",
                  "20090101", "20090401", "20090701", "20091001",
                  "20100101", "20100401", "20100701", "20101001",
                  "20110101", "20110401", "20110701", "20111001",
                  "20120101", "20120401", "20120701", "20121001",
                  "20130101", "20130401", "20130701", "20131001",
                  "20140101", "20140401", "20140701", "20141001",
                  "20150101", "20150401", "20150701", "20151001",
                  "20160101"]
#
#
# len(period_3yr_lst)
#
cutoff_date = ["20041231", "20050331", "20050630", "20050930",
               "20051231", "20060331", "20060630", "20060930",
               "20061231", "20070331", "20070630", "20070930",
               "20071231", "20080331", "20080630", "20080930",
               "20081231", "20090331", "20090630", "20090930",
               "20091231", "20100331", "20100630", "20100930",
               "20101231", "20110331", "20110630", "20110930",
               "20111231", "20120331", "20120630", "20120930",
               "20121231", "20130331", "20130630", "20130930",
               "20131231", "20140331", "20140630", "20140930",
               "20141231", "20150331", "20150630", "20150930",
               "20151231", "20160331", "20160630", "20160930",
               "20161231", "20170331", "20170630", "20170930",
               "20171231", "20180331", "20180630", "20180930",
               "20181231"]
#
# len(cutoff_date)
#
quarter_end_date = ["20050331", "20050630", "20050930", "20051231",
                    "20060331", "20060630", "20060930", "20061231",
                    "20070331", "20070630", "20070930", "20071231",
                    "20080331", "20080630", "20080930", "20081231",
                    "20090331", "20090630", "20090930", "20091231",
                    "20100331", "20100630", "20100930", "20101231",
                    "20110331", "20110630", "20110930", "20111231",
                    "20120331", "20120630", "20120930", "20121231",
                    "20130331", "20130630", "20130930", "20131231",
                    "20140331", "20140630", "20140930", "20141231",
                    "20150331", "20150630", "20150930", "20151231", 
                    "20160331", "20160630", "20160930", "20161231",
                    "20170331", "20170630", "20170930", "20171231",
                    "20180331", "20180630", "20180930", "20181231",
                    "20190331"]

# len(quarter_end_date)
#x = data_collector('2005-12-31 00:00:00', "20030101", "20051231")
#a = x[4]
#a.to_csv("test72.csv")
#
#b = x[0]
#b.to_csv("test73.csv")
#
#c = x[1]
#d = x[2]


# len(quarter_end_date)

master_date_lst = []
for i in range(0, 57):
    lst = [rebalancing_date_lst[i], period_3yr_lst[i], cutoff_date[i], quarter_end_date[i]]
    master_date_lst.append(lst)

len(master_date_lst)

# master_date_lst[0]

df = pd.DataFrame()
count = 0
for dates in master_date_lst[45:]:
    rebalancing_date = dates[0]
    period_3yr = dates[1]
    cutoff_date = dates[2]
    quarter_end = dates[3]
    
#    try:
    df_portreturn = backtest_indexweight(rebalancing_date, period_3yr, cutoff_date, quarter_end)
    df = df.append(df_portreturn)
    count += 1
    print ("No. of loops completed: {0}".format(count))
    time.sleep(3)
    print ("Restarting.....")


df

# cd T:\Razin.Hussain\low_vol_global_equity\backtest_results
df.to_csv("ret_newseries(6).csv")



























#############################################################################################################


df = pd.DataFrame()
master_date_lst = [['2004-12-31 00:00:00', "20020101", "20041231", "20050331"], ['2005-03-31 00:00:00', "20020401", "20050331", "20050630"],
                   ['2005-06-30 00:00:00', "20020701", "20050630", "20050930"]]


## Q3 2018 - Working
rebalancing_date = "2018-09-30 00:00:00"
period_3yr = "20151001"
cutoff_date = "20180930"
quarter_end = "20181231"


## Q2 2018 - Working
rebalancing_date = "2018-06-30 00:00:00"
period_3yr = "20150701"
cutoff_date = "20180630"
quarter_end = "20180930"


port = optimal_portfolio(rebalancing_date, period_3yr, cutoff_date)
x = port[2]
x.to_csv("test78.csv")

test = backtest(rebalancing_date, period_3yr, cutoff_date, quarter_end)
test


a = port[0]
a
b = port[1]
b.to_csv("test77.csv")

b.to_csv("test76.csv")




lst = []
start = "20020101"
for i in range (0, 4):
    x = int(start)
    x = x + 300
    x = str(x)
    lst.append(x)

print (lst)    



for i in range (0,16):
    year = int(start[1:4]) + 1
    if year > 9:
        year = "0" + str(year)
    else:
        year = "00" + str(year)
        
    new = start.replace(start[1:4], year)
    lst.append(new)
    start = new

print (lst)





# First quarter
rebalancing_date = "2006-06-30 00:00:00"
d = datetime.datetime.strptime(rebalancing_date, "%Y-%m-%d %H:%M:%S")
d1 = (d + datetime.timedelta(round((3*368)/12, 0)))

d
d1 = d1.strftime("%Y-%m-%d %H:%M:%S")

period_3yr_lst = ["20020101", "20020401", "20020701", "20021001", "20030101", ]





period_3yr = "20020101"
#period_3yr[4:6]

if period_3yr[4:6] == "01":
    period_3yr[4:6] = period_3yr.replace(period_3yr[4:6], "04")
    
elif period_3yr[4:6] == "04":
    period_3yr = period_3yr.replace(period_3yr[4:6], "07")
elif period_3yr[4:6] == "07":
    period_3yr = period_3yr.replace(period_3yr[4:6], "10")
elif period_3yr[4:6] == "10":
    year = int(period_3yr[1:4]) + 1
    if year > 9:
        year = "0" + str(year)
        month = "01"
        period_3yr = period_3yr.replace(period_3yr[1:6], year + month)
    else:
        year = "00" + str(year)
        month = "01"
        period_3yr = period_3yr.replace(period_3yr[1:6], year + month)

print (period_3yr)    

    

period_3yr = period_3yr.replace()


cutoff_date = "20041231"
quarter_start = "20050101"
quarter_end = "20050331"


# Second quarter
rebalancing_date = "2005-03-31 00:00:00"
period_3yr = "20020401"
cutoff_date = "20050331"

# Be wary of the dates, for example June does not have 31 days so putting 31 in dates will turn an error
# quarter_start = "20050331"
quarter_end = "20050630"

### Working
rebalancing_date = "2018-09-30 00:00:00"
period_3yr = "20151001"
cutoff_date = "20180930"

# Be wary of the dates, for example June does not have 31 days so putting 31 in dates will turn an error
# quarter_start = "20050331"
quarter_end = "20181231"


### Not 
rebalancing_date = "2018-06-30 00:00:00"
period_3yr = "20150701"
cutoff_date = "20180630"

# Be wary of the dates, for example June does not have 31 days so putting 31 in dates will turn an error
# quarter_start = "20050331"
quarter_end = "20180930"


###
rebalancing_date = "2018-03-31 00:00:00"
period_3yr = "20150401"
cutoff_date = "20180330"

# Be wary of the dates, for example June does not have 31 days so putting 31 in dates will turn an error
# quarter_start = "20050331"
quarter_end = "20180630"



x = backtest(rebalancing_date, period_3yr, cutoff_date, quarter_end)

x.to_csv("ret1.csv")




ret = x[0]
weight = x[1]

ret.shape
weight.shape

ret.to_csv("test63.csv")
weight.to_csv("test64.csv")


len(ret)
weight

df = pd.DataFrame({"date": ["2015-12-31", "2014-12-15", "2017-08-18"], "AKS": [2.5, 6.8, 2.5], "FGD": [23.5, 26.8, 22.8]})
df["date"] = pd.to_datetime(df.date)
df = df.sort_values(by = "date")

df


    for bb_ticker in ticker_lst:
        query = '''
        select symbol.id, data.price_date, symbol.bb_ticker, data.daily_return from symbol inner join data on symbol.id = data.symbol_id where price_date between "2005-01-01 00:00:00" and "2005-12-31 00:00:00"
        '''
        df = pd.read_sql_query(query, engine)
        df = df.rename(columns = {"daily_return": bb_ticker})
        cols_to_keep = ["price_date", bb_ticker]
        df = df[cols_to_keep]
        
        df_dailyreturn_lst.append(df)
    
    df_dailyreturn = reduce(lambda left, right: pd.merge(left, right, how = "inner", on = "price_date"), df_dailyreturn_lst)
    df_dailyreturn = df_dailyreturn.set_index("price_date")
    
    # Calculating covariance matrix and annualized return matrix
    cov_matrix = df_dailyreturn.cov()
    annual_return = df_dailyreturn.mean()*252
    return_matrix = annual_return.as_matrix()
    
    return df_dailyreturn, cov_matrix, return_matrix, ticker_lst, quant_basket

#def append_return(rebalancing_date, period_3yr, cutoff_date, quarter_start, quarter_end):
#    df_portreturn = backtest(rebalancing_date, period_3yr, cutoff_date, quarter_start, quarter_end)
#    df = pd.DatFrame()
#    df = df.append(df_portreturn)
#    
#    return df
#

#### Example recursive function to restart completed loops (VV Imp) 
#def main(required_value):
#    count = 0
#    for i in lst:
#        count += 1
#        print (count)
#        j = lst[count]
#        if (((i - required_value > 0.0) and (j - required_value > 0.0)) or ((i - required_value < 0.0) and (j - required_value < 0.0))):
#            if count < (len(lst) - 1):
#                continue
#            elif count == (len(lst) - 1):
#                required_value += 0.5
#                print ("Adding 0.5 to required_value, new required value: {0}".format(required_value))
##                x = input("Do you want to restart the loop: ")
##                if x == "yes":
#                main(required_value)
#        else:
#            right = required_value - j
#            left = i - required_value
#            if right < left:
#                print ("Finally found the optimal value, {0}, in the right, {1}".format(j, right))
#                break
#            elif left < right:
#                print ("Finally found the optimal value, {0} in the left: {1}".format(i, left))
#                break
#        break
#    return
#
#
#
#main(1.5)
#


#def index_weight(rebalancing_date):
#    df = basket(rebalancing_date)
#    df["index_weight"] = df["index_weight"].apply(lambda x: (x/df["index_weight"].sum()))
#    
#    count = 0
#    for weight in df["index_weight"]:
#        upper_limit = 0.040
#        if weight > upper_limit:
#            df.iloc[count:count+1, 6:7] = upper_limit
#            increment = (weight - upper_limit)/len(df)
#            df["index_weight"] = df["index_weight"] + increment
#        count = count + 1    
#    
#    return df
#
#
#def backtest_indexweight(rebalancing_date, period_3yr, cutoff_date, quarter_end):
#    data = index_weight(rebalancing_date)
##    df_weight = data[6]
#    df_weight = data[["bb_ticker", "index_weight"]]
#
#    # Reducing weight by 1 percent to accomodate cash
#    df_weight["index_weight"] = df_weight["index_weight"].apply(lambda x: (x/df["index_weight"].sum()) * 0.990) 
#
#    # Removing cash from the dataframe
##    df_weight = df_weight.iloc[1:]
#    
#    ticker_lst = [ticker for ticker in df_weight["bb_ticker"]]
#    df_returnlst = []
#    df_divlst = []
#    for ticker in ticker_lst:
#        try:
#            df = con.bdh(ticker, ["PX_LAST", "EQY_DVD_YLD_12M"], cutoff_date, quarter_end)
#        except:
#            continue
#        df.columns = df.columns.droplevel("field")
#        df = df.reset_index()
#        if len(df.columns) == 2:
##            div_ticker = ticker.replace(ticker[:], ticker[0:6])
#            df[ticker.replace(ticker[:], ticker[0:6])] = [0.0 for i in range (len(df))]
#        
#        df_price = df.iloc[:, [0, 1]]
#        df_price = df_price.set_index("date")
#        
#        try:
#            df_price.columns = [ticker]
#        except:
#            df_weight = df_weight.set_index("bb_ticker")
#            df_weight = df_weight.drop([ticker])
#            df_weight = df_weight.reset_index()
#            continue
#        
#        df_price = df_price.pct_change(1)
#        df_price = df_price.iloc[1:]
#        df_price = df_price.reset_index()
#        df_returnlst.append(df_price)
#        
#        df_div = df.iloc[:, [0, 2]]
#        df_div = df_div.set_index("date")
#        df_div = df_div.iloc[1:]
#        df_div = df_div.apply(lambda x: (x/100)/252)
#        df_div = df_div.reset_index()
#        df_divlst.append(df_div)
#    
#    df_dailyreturn = reduce(lambda left, right: pd.merge(left, right, how = "outer", on = "date"), df_returnlst)
#    df_dailydiv = reduce(lambda left, right: pd.merge(left, right, how = "outer", on = "date"), df_divlst)
#
#    df_dailyreturn = df_dailyreturn.fillna(0)
#    df_dailyreturn = df_dailyreturn.sort_values(by = "date")
#    
#    df_dailydiv = df_dailydiv.fillna(0)
#    df_dailydiv = df_dailydiv.sort_values(by = "date")
#    
#    df_dailyreturn = df_dailyreturn.set_index("date")
#    return_matrix = df_dailyreturn.as_matrix()
#    
#    df_dailydiv = df_dailydiv.set_index("date")
#    div_matrix = df_dailydiv.as_matrix()
#
#    df_weight = df_weight.set_index("bb_ticker")
#    weight_matrix = df_weight.as_matrix()
#    
#    df_dailyreturn = df_dailyreturn.reset_index()
#    df_dailydiv = df_dailydiv.reset_index()
#    
#    capital_gain = np.dot(return_matrix, weight_matrix)
#    div_yield = np.dot(div_matrix, weight_matrix)
#    
#    df_portreturn1 = pd.DataFrame()
#    df_portreturn1["date"] = df_dailyreturn["date"]
#    df_portreturn1["port_capitalgain"] = capital_gain
#    df_portreturn2 = pd.DataFrame()
#    df_portreturn2["date"] = df_dailydiv["date"]
#    df_portreturn2["port_divyield"] = div_yield
#    df_portreturn = pd.merge(df_portreturn1, df_portreturn2, how = "outer", on = "date")
#    df_portreturn = df_portreturn.sort_values(by = "date")
#    df_portreturn = df_portreturn.fillna(0)
#    
#    df_portreturn["port_totalreturn"] = df_portreturn["port_capitalgain"] + df_portreturn["port_divyield"]
#
##    df_portreturn["div_yield"] = div_yield
##    df_portreturn["port_totalreturn"] = df_portreturn["port_capitalgain"] + df_portreturn["div_yield"]
#    
##    df = pd.DataFrame()
##    df = df.append(df_portreturn, ignore_index = True)
#    
#    return df_portreturn
#
#
#

    
