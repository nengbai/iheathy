import es_stocks as es
import to_Json_Yaml as tf
import os,json

if __name__ == '__main__':
    current_path = os.path.abspath(".")
    yml_file = os.path.join(current_path, "config-bn.yaml")
    file = open(yml_file, 'r', encoding="utf-8")
    file_data = file.read()
    file.close()
    
    # Ready index map setting
    data_list = tf.yaml_toJson(file_data)
    for i in range(0,len(data_list)):
        if i == 0:
            index_map = data_list[0]
            #print(index_map)
        else:
            file_list = json.loads(data_list[1])
            filename = file_list['file']['filename']
            pathDir = file_list['file']['pathDir']
            index_name = file_list['index']['index_name']
            index_type = file_list['index']['index_type']
        
    # Ready for load data from and input to put_list
    if os.path.exists(pathDir + '/' + filename):
        with open(pathDir + '/' + filename,'r+',encoding='utf-8') as file:
            input = file.read()
            file.close()
    else:
        print(filename + '文件在' + pathDir + '中不存在 ! ')
    input = json.loads(input)
    
    put_list = []
    for i in range(0,len(input)):
        put_list.append(input[i])
    
    # Ready for create index and load data to ES
    obj = es.Search(index_name, index_type)
    obj.create_index(index_name,index_type,index_map)
    obj.bulk_Index_Data(put_list)
    
