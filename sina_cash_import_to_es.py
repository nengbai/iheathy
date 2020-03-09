__author__: "Bai Neng"
import es_stocks_cash as es
import to_Json_Yaml as tf
import urllib.request
import tushare as ts
import re,json,sys,os
import datetime,time
import logger as lg

def sinaStockUrl( max_pg ):
    
    #print( 'pageNum : ' + str( pageNum ))
    #http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_bkzj_ssggzj?page=38
    max_pg = max_pg + 1
    rows = 100
    url_list = []
    pageNum = 0
    for pageNum in range(max_pg) :
        url = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_bkzj_ssggzj?'
        url += 'page=' + str( pageNum )
        #url += '&num=' + str( rows )
        #url += '&sort=symbol&asc=1&node=hs_a&symbol=&_s_r_a=init'
        pageNum = pageNum + 1 
        url_list.append(url)
    return url_list

def sinaStockData(url_list):
    
    # http://www.cnblogs.com/sysu-blackbear/p/3629420.html
    stockData = []
    for url in range(0,len(url_list)):
        #print(url_list[url])
        histData = urllib.request.urlopen(url_list[url]).read()
        histData = histData.decode('gbk')
        histData = str(histData).split('[')[1]
        histData = histData[1:len(histData) - 4].split('},{')
        for i in range(0, len(histData)):
            column = {}
            dayData = histData[i].split(',')
            for j in range(0, len(dayData)):
                field = dayData[j].split(':')
                try:
                    column[field[0]] = field[1].replace('"', '')
                except:
                    continue
            stockData.append(column) 
    return stockData
        
if __name__ == '__main__':
    if (len(sys.argv) <2):
        print('Please input paramente:trade_date,format [yyyy.mm.dd] and load_time ,format [yyyy.mm.dd hh:mm:ss]')
        exit("sorry,please input correct parameter")
    else:
        trade_date = sys.argv[1]
        load_time = sys.argv[2]
    #trade_date = '2020.02.28'
    #load_time = '2020-02-28 12:00:00'
    
    # 1. check if it's trade date for input trade_date
    #ts.set_token('39dc678ce257d84ee9d3eeb5c404f0df39089b3c8f9adb83b247253a')
    #pro = ts.pro_api()
    #trade = pro.query('trade_cal', start_date=trade_date, end_date=trade_date)
    #trade = trade.to_dict(orient='records')
    #if trade[0]['is_open'] == 1: 
    #else:
    #    print("Sorry,today:%s isn't trade date:",trade_date, )
        
    # 2. get stock raw data from sina url link and append items: load_time and trade_date
    url = sinaStockUrl(66)
    str_stocks = sinaStockData(url)
    print(str_stocks)
    
    put_list = []
    for i in range(0,len(str_stocks)):
        li = str_stocks[i]
        li['insert_time'] = load_time
        li['trade_date'] = trade_date
        put_list.append(li)
        
    # 3. read from yaml config 
    current_path = os.path.abspath(".")
    yml_file = os.path.join(current_path, "config-sina-cash.yaml")
    file = open(yml_file, 'r', encoding="utf-8")
    file_data = file.read()
    file.close()
    
    # 4. Ready for index mapping and log setting
    data_list = tf.yaml_toJson(file_data)
    for i in range(0,len(data_list)):
        if i == 0:
            index_map = data_list[0]
            print(index_map)
        else:
            file_list = json.loads(data_list[1])
            index_name = file_list['index']['index_name']
            index_name = index_name + trade_date
            index_type = file_list['index']['index_type']
            logname = file_list['log']['logname']
            log_level = file_list['log']['level']
            
    log = lg.Logger(logname,level=log_level)
    
    # 5. Ready to load stock data to ES
    obj = es.Search(index_name, index_type)
    if obj == None:
        log.logger.info("index:%s isn't exit",index_name)
        obj.create_index(index_name,index_type,index_map)
        obj.bulk_Index_Data(put_list)
        log.logger.info("create index:%s",index_name)
    else:
        obj.bulk_Index_Data(put_list)
        log.logger.info("index:%s is exit",index_name)
    
