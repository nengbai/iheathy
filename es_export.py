__author__: "Bai Neng"
import es_stocks as es
import to_Json_Yaml as tf
import os,sys,json
if __name__ == '__main__':
    if (len(sys.argv) <1):
        print('Please input paramente:trade_date,format [yyyy.mm.dd] ')
        exit("sorry,please input correct parameter")
    else:
        trade_date = sys.argv[1]
    #trade_date = '2020.02.28'
    current_path = os.path.abspath(".")
    yml_file = os.path.join(current_path, "config.yaml")
    file = open(yml_file, 'r', encoding="utf-8")
    file_data = file.read()
    file.close()
    
    # 1. Ready index map setting
    data_list = tf.yaml_toJson(file_data)
    for i in range(0,len(data_list)):
        if i == 0:
            body = json.loads(data_list[0])
            body['query']['match']['trade_date'] = trade_date
        else:
            file_list = json.loads(data_list[1])
            filename = file_list['file']['filename']
            pathDir = file_list['file']['pathDir']
            index_name = file_list['index']['index_name']
            index_name = index_name + trade_date
            index_type = file_list['index']['index_type']
            
    # 2. Ready for read data from ES and output to get_list
    obj = es.Search(index_name, index_type)
    get_list = obj.Get_Data_batch(trade_date,body)
    
    # 3. Ready for read data from ES and output to get_list
    put_list = []
    for i in range(0,len(get_list)):
        put_list.append(get_list[i][0])
    
    output = json.dumps(put_list)
    if os.path.exists(pathDir + '/' + filename):
       print(filename + '文件在' + pathDir + '中存在 ! ')
    else:
        with open(pathDir + '/' + filename,'w',encoding='utf-8') as file:
            file.write(output)
            file.close()
