__author__: "Bai Neng"
import es_stocks_cash as es
import re,json,sys,os
import datetime,time
if __name__ == '__main__': 
    '''
    1. 
    '''
    index_name = "cn_stocks_cashflow-2020.03.18"
    index_type = "_doc"
    trade_date = "2020.03.24"
    body = {
        "query":{
            "range":{
                "insert_time": {
                    "gte": "2020-03-18 00:00:00",
                    "lte": "2020-03-19 00:00:00"
                    }
                } 
        },
        "sort": {
            "symbol.keyword": {
            "order": "asc"
              }
         }
    }
    '''
     2. Query data from ES and general delete list
    '''
    obj = es.Search(index_name, index_type)
    get_list = obj.get_data_batch(trade_date,body)
    id_dict ={}
    temp = ''
    dl = []
    dl_list = []
    for i in get_list:
        id_dict = {"symbol": i[0]['symbol'],"id" : i[1]}
        dl.append(id_dict)
    for i in range(0,len(dl)):
        id = dl[i]['id']
        symbol = dl[i]['symbol']
        #print("symbol:%s,id:%s" %(symbol,id))
        temp1 = symbol
       # print("----- %s" %temp1)
        if temp == symbol :
            dl_list.append(id)
        temp = temp1
        #print("++++ %s" %dl_list)

    '''
    3. Batch delete duplication data
    ''' 
    obj.delete_index_data(dl_list)
