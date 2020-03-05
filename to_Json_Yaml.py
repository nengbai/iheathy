import yaml
import os
import demjson
import json

def json_toPropt(astr):
    return astr.replace(' ','').replace('\n','').replace('\r','')\
               .replace("'",'"').replace('{','{"').replace(':','":')\
               .replace('],','],"').replace('",','","')


def json_toYaml(file_data,output=""):
    data = demjson.decode(file_data)
    data= json.dumps(data)
    yaml_data = yaml.load(data,Loader=yaml.FullLoader)
    #yml_file = output
    #stream = open(yml_file, 'w')
    #data = yaml.safe_dump(yaml_data,stream,default_flow_style=False)
    #print('please check yaml file:',yml_file)
    return yaml_data

def yaml_toJson(file_data,output=""):
    data = yaml.load_all(file_data,Loader=yaml.FullLoader)
    data_list = [ ]
    for data in data:
        json_data = json.dumps(data)
        data_list.append(json_data)
        json_file= output
        #stream = open(json_file, 'a')
        #stream.write(json_data)
        #print(json_data)
    return data_list
