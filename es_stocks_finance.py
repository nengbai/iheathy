__author__: "Bai Neng"
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch import helpers
 

class Search(object):

    def __init__(self,index_name,index_type, ip=''):
        self.index_name = index_name
        self.index_type = index_type
        self.es = Elasticsearch([ip],http_auth=('username', 'password'), port=9200)

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
        #self.action = action
        ACTIONS = []
        for line in self.list:
            try: 
                action = {
                   "_index": self.index_name,
                   "_type": self.index_type,
                     #"_id": i, #_id 也可以默认生成，不赋值
                     "_source": {
                      "load_time": line['load_time'],
                       "symbol": line['symbol'],
                       "name": line['name'],
                       "ts_code": line['ts_code'],
                       "area": line['area'],
                       "industry": line['industry'],
                       "quarter": line['quarter'],
                       "每股净资产-摊薄/期末股数": line['每股净资产-摊薄/期末股数'],
                       "每股现金流": line['每股现金流'],
                       "每股资本公积金": line['每股资本公积金'],
                       "固定资产合计": line['固定资产合计'],
                       "流动资产合计": line['流动资产合计'],
                       "资产总计": line['资产总计'],
                       "长期负债合计": line['长期负债合计'],
                       "主营业务收入": line['主营业务收入'],
                       "财务费用": line['财务费用'],
                       "净利润": line['净利润']
                       }
                     }
                
                ACTIONS.append(action)
                print(ACTIONS)
            except:
                continue
                    
            # 批量处理
        success, _ = bulk(self.es, ACTIONS, index=self.index_name, raise_on_error=True)
        print('Performed %d actions' % success)
        
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
                time.sleep(5)
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
