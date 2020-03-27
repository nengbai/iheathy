__author__: "Bai Neng"
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch import helpers
import logger as lg
import json,sys,os,datetime
import to_Json_Yaml as tf


#  read from yaml config and set log
current_path = os.path.abspath(".")
yml_file = os.path.join(current_path, "config-sina-finance-bn.yaml")
file = open(yml_file, 'r',encoding='utf-8')
file_data = file.read()
file.close()

log_list = tf.yaml_toJson(file_data)
for i in range(0,len(log_list)):
    if i == 1:
        log_list = json.loads(log_list[1])
        logname = log_list['log']['logname']
        log_level = log_list['log']['level']
log = lg.Logger(logname,level=log_level)



class Search(object):

    def __init__(self,index_name,index_type, ip='192.168.1.34'):
        self.index_name = index_name
        self.index_type = index_type
        self.es = Elasticsearch([ip],http_auth=('elastic', 'admin@123'), port=9200)

    def create_index(self, index_name="", index_type="",index_map=""):
        """
        创建索引, 创建索引名字
        :param ex: Elasticsearch对象
        :return:
        """
        self.index_name = index_name
        self.index_type = index_type
        self.index_map = index_map
        
        # 索引 相当于数据库中的 库名
        _index_mappings = self.index_map
        if self.es.indices.exists(index=index_name) is not True:
            res = self.es.indices.create(index=index_name,body=_index_mappings)
            log.logger.info(res)
            
    def bulk_index_data(self,put_list,key_list):
        '''
        用bulk将批量数据存储到es
        
        :return:
        '''
        tmp = []
        ACTIONS = []
        '''
        Genenal source format dict source { 'a' : 'a'},then compare with put_list.keys().
        '''
        for i in range(0,len(key_list)):
            if len(key_list[i]) >0:
                value = key_list[i]
                tmp.append(value)
                print(value)
        source = dict(zip(key_list,tmp))
        #log.logger.info(key_list)
        log.logger.info(source)
        for line in range(0,len(put_list)):
            for key in put_list[line].keys():
                if source[key] == key and len(source[key])>0 :
                    source[key] = put_list[line][key]   
            try: 
                action = {
                    "_index": self.index_name,
                    "_type": self.index_type,
                    #"_id": i, #_id 也可以默认生成，不赋值
                    "_source": source
                        }
                
                ACTIONS.append(action)
                #log.logger.info(ACTIONS)
            except:
                continue
                    
                # 批量处理
        success, _ = bulk(self.es, ACTIONS, index=self.index_name, raise_on_error=True)
        #log.logger.info('Performed %d actions' % success)
        
    
    def update_data_byBody(self,doc, id):
        '''
        update data by id
        doc: geneal query condition by Get_Data_By_Body
        id: document id
        '''
        if id != 0:
            self.id = id
            self.doc = doc
            log.logger.info("id:",self.id, "and query is :", self.doc)
            result = self.es.update(index=self.index_name, id = self.id ,body = self.doc)
        else:
            log.logger.info("This is zero parameter")
        
    def update_bult_byQuery(self,get_list):
        """
        batch update funcation for add new items and update value
        create date: 2020-03-07
        """
        ACTIONS = []
        #print(get_list)
        for line in range(0,len(get_list)):
            #print(get_list[line]['industry'])
            action = {
                '_op_type' : 'update',    # index update create delete  
                '_index' : self.index_name, #index
                '_type' : self.index_type,  #type
                '_id' : get_list[line]['id'],
                'doc' : {
                    'area' : get_list[line]['area'],
                    'industry': get_list[line]['industry']
                    }
                }
            log.logger.info(action) 
            ACTIONS.append(action)
        success, _ = bulk(self.es,ACTIONS,index=self.index_name, raise_on_error=True)
        log.logger.info('Performed %d actions' % success)
        #for ok,response in streaming_bulk(self.es,ACTIONS,index=self.index_name, raise_on_error=True):
        #    if not ok:
        #        print(response)
    
    def update_data_byQuery(self, query, data_list):
        """
        use to add new filed 
        query dict: query condition follow the ES DSL standard rules
        body = {"match": {
                "keyword": "network viedio"
                }
            }
        field str: update item 
        value str: replace value
        
        """
        results = []
        for i in range(0,len(data_list)):
            for key,values in  data_list[i].items():
                #print(key,values)
                field = key
                value = values
                script = "ctx._source." + field + " = '" + value + "'"
                body = {"query": query, "script": script}
                log.logger.info("query is : ", body)
                result = self.es.update_by_query(index=self.index_name, body = body)
                # add sleep to resolution "version_conflict_engine_exception" when update the same doc
                time.sleep(5)
                results.append(result)
        return results
    
    def delete_index_data(self,dl_list):
        '''
        Update by Bai Neng on March 26th,2020: batch delete item by id.
        :param id:
        :return:
        '''        
        ACTIONS = []
        for line in dl_list:
            action = {
                '_op_type' : 'delete',    # index update create delete  
                '_index' : self.index_name, #index
                '_type' : self.index_type,  #type
                '_id' : line,
                'doc' : { }
                }
            #log.logger.info(action) 
            ACTIONS.append(action)
        success, _ = bulk(self.es,ACTIONS,index=self.index_name, raise_on_error=True)
        log.logger.info('Performed %d actions' % success)
    
    def get_data_byinput(self,body, field,value):
        '''
        find data from ES
        '''
        self.body = body
        self.field = field
        self.value = value
        #script = "ctx._source." + self.field + " = '" + self.value + "'"
        _searched = self.es.search(index=self.index_name,body=self.body,scroll='5m',size=5000)
        results = _searched['hits']['hits']
        total = _searched['hits']['total']['value']
        scroll_id = _searched['_scroll_id']
        #print(_searched, flush=True)
        get_list = []
        for i in range(0, int(total/100)+1):
            if  total > 0:
                query_scroll = self.es.scroll(scroll_id=scroll_id,scroll='5m')['hits']['hits']
                results += query_scroll
                for hit in results:
                    if hit['_source']['trade_date'] == trade_date:
                       record = {"doc": hit['_source']}
                       record =  record["doc"]
                       id = hit['_id']
                       get_list.append([record,id])
                    else:
                        print(hit['_source'])
                        return 0
                return get_list
            else:
                print("ES query execute error")
                return 0
        
    def get_data_batch(self,trade_date,body):
        '''
        update by Bai Neng: on March 26,2020 for batch delete query
        find data from ES
        '''
        self.trade_date = trade_date
        self.body = body
        _searched = self.es.search(index=self.index_name,body=self.body,scroll='5m',size=500)
        results = _searched['hits']['hits']
        total = _searched['hits']['total']['value']
        scroll_id = _searched['_scroll_id']
        #print(_searched, flush=True)
        #print(results)
        get_list = []
        for i in range(0, int(total/100)+1):
            if  total > 0:
                query_scroll = self.es.scroll(scroll_id = scroll_id)['hits']['hits']
                results += query_scroll
                for hit in results:
                   # if hit['_source']['trade_date'] == trade_date:
                    record = {"doc": hit['_source']}
                    record =  record["doc"]
                    id = hit['_id']
                    get_list.append([record,id])
                    #else:
                    #    print(hit['_source'])
                    #    return 0
                return get_list
            else:
                log.logger.info("ES query execute error")
                return 0
            
    def get_bult_batch(self,data_list):
        '''
        The funcation is to use genaral bult app update data
        create date: 2020-03-07
        '''
        get_list = []
        for i in range(0,len(data_list)):
            body =  data_list[i]['body']
            symbol = data_list[i]['symbol']
            area = data_list[i]['area']
            industry = data_list[i]['industry']
            _searched = self.es.search(index=self.index_name,body=body,scroll='5m',size=500)
            results = _searched['hits']['hits']
            total = _searched['hits']['total']['value']
            scroll_id = _searched['_scroll_id']
            
            for i in range(0, int(total/100)+1):
                if  total > 0:
                    query_scroll = self.es.scroll(scroll_id = scroll_id)['hits']['hits']
                    results += query_scroll
                    for hit in results:
                        if hit['_source']['symbol'] == symbol:
                            record = {"doc": hit['_source']}
                            record =  record["doc"]
                            id = hit['_id']
                            temp = {'id': id,'area':area,'industry':industry}
                            get_list.append(temp)
                        else:
                            log.logger.info(hit['_source'])
                            return 0
        return get_list
    
    
