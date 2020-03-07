__author__: "Bai Neng"
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import time
 

class Search(object):

    def __init__(self,index_name,index_type, ip='192.168.1.33'):
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
            print(res)
            
    def bulk_Index_Data(self,put_list):
        '''
        用bulk将批量数据存储到es
        
        :return:
        '''
        self.list = put_list
        ACTIONS = []
        for line in self.list:
            try:
                action = action = {
                   "_index": self.index_name,
                   "_type": self.index_type,
                   #"_id": i, #_id 也可以默认生成，不赋值
                   "_source": {
                       "insert_time": line['insert_time'],
                       "trade_date": line['trade_date'],
                       "symbol": line['symbol'],
                       "name": line['name'],
                       "trade": line['trade'],
                       "changeratio": line['changeratio'],
                       "turnover": line['turnover'],
                       "amount": line['amount'],
                       "inamount": line['inamount'],
                       "outamount": line['outamount'],
                       "netamount": line['netamount'],
                       "ratioamount": line['ratioamount'],
                       "r0_in": line['r0_in'],
                       "r0_out": line['r0_out'],
                       "r0_net": line['r0_net'],
                       "r3_in": line['r3_in'],
                       "r3_out": line['r3_out'],
                       "r3_net": line['r3_net'],
                       "r0_ratio": line['r0_ratio'],
                       "r3_ratio": line['r3_ratio'],
                       "r0x_ratio": line['r0x_ratio']
                       }
                }
                print(action)
                ACTIONS.append(action)
            except:
                    continue
            # 批量处理
        success, _ = bulk(self.es, ACTIONS, index=self.index_name, raise_on_error=True)
        print('Performed %d actions' % success)
    
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
            print(action) 
            ACTIONS.append(action)
        success, _ = bulk(self.es,ACTIONS,index=self.index_name, raise_on_error=True)
        print('Performed %d actions' % success)
        #for ok,response in streaming_bulk(self.es,ACTIONS,index=self.index_name, raise_on_error=True):
        #    if not ok:
        #        print(response)
    
            
    def update_data_byBody(self,doc, id):
        '''
        update data by id
        doc: geneal query condition by Get_Data_By_Body
        id: document id
        '''
        if id != 0:
            self.id = id
            self.doc = doc
            print("id:",self.id, "and query is :", self.doc)
            result = self.es.update(index=self.index_name, id = self.id ,body = self.doc)
        else:
            print("This is zero parameter")
        
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
                print("query is : ", body)
                result = self.es.update_by_query(index=self.index_name, body = body)
                # add sleep to resolution "version_conflict_engine_exception" when update the same doc
                time.sleep(3)
                results.append(result)
        return results
    
    
    
    def delete_index_data(self,id):
        '''
        delete item by id
        :param id:
        :return:
        '''
        res = self.es.delete(index=self.index_name, doc_type=self.index_type, id=id)
        print(res)
    
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
        find data from ES
        '''
        self.trade_date = trade_date
        self.body = body
        _searched = self.es.search(index=self.index_name,body=self.body,scroll='5m',size=5000)
        results = _searched['hits']['hits']
        total = _searched['hits']['total']['value']
        scroll_id = _searched['_scroll_id']
        #print(_searched, flush=True)
        #print(results)
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
    def get_bult_batch(self,data_list):
        '''
        The funcation is to use genaral bult app update data
        create date: 2020-03-07
        '''
        get_list = []
        for i in range(0,len(data_list)):
            print(data_list[i])
            body =  data_list[i]['body']
            symbol = data_list[i]['symbol']
            area = data_list[i]['area']
            industry = data_list[i]['industry']
            _searched = self.es.search(index=self.index_name,body=body,scroll='5m',size=500)
            results = _searched['hits']['hits']
            total = _searched['hits']['total']['value']
            scroll_id = _searched['_scroll_id']
            print("scroll_id is:%s",scroll_id)
            
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
                            print(hit['_source'])
                            return 0
        return get_list
