# -*- coding: utf-8 -*-
"""
Created on Fri Dec  6 15:53:03 2019

@author: robert.currie
"""

import pandas as pd
import numpy as np
import blpapi
import pdblp
from functools import reduce


con = pdblp.BCon(debug = False, port = 8194, timeout = 50000)
con.start()

cd T:\Razin.Hussain\low_vol_global_equity\Archive


def backtest(cutoff_date, quarter_end):
    data = pd.read_csv("port_20190930.csv")    
    df_weight = data[["bb_ticker", "optimal_weight"]]
    
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

ret = backtest("20190927", "20191231")

cd T:\Razin.Hussain\low_vol_global_equity\backtest_results
ret.to_csv("ret_20191001-20191231.csv")



