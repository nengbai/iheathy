__author__: "Bai Neng"
import es_stocks_cash as es
import to_Json_Yaml as tf
import urllib.request
import tushare as ts
import re,json,sys,os
import datetime,time

if __name__ == '__main__':    
    # 1. input index_name
    if (len(sys.argv) <1 ):
        print('Please input paramente:index_name : format is cn_stocks_cashflow-[YYYY.MM.DD]]')
        exit("sorry,please input correct parameter")
    else:
        index_name = sys.argv[1]
    # 2. check if it's trade date for input trade_date
    ts.set_token('39dc678ce257d84ee9d3eeb5c404f0df39089b3c8f9adb83b247253a')
    pro = ts.pro_api()
    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry')
    data_records = data.to_dict(orient='records')
   
    #index_name = "cn_stocks_cashflow-2020.03.04"
    index_type = "_doc"
    
    # 5. Ready to load stock data to ES
    obj = es.Search(index_name, index_type)
    data_list = []
    for i in range(0,len(data_records)):
        symbol = data_records[i]['ts_code'][-2:].lower()+data_records[i]['ts_code'][:-3]
        temp ={'symbol':symbol,'area': data_records[i]['area'],'industry': data_records[i]['industry'],'body':{"query":{"match": {"symbol": symbol}}}}
        data_list.append(temp)  
    #print(data_list)
    get_list = obj.get_bult_batch(data_list)
    #print(get_list)
    results = obj.update_bult_byQuery(get_list)
