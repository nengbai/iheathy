# iheathy is to use stock market aninalysis platform base on Elastic Search and python language. it collects trade date and show user analysis dashboard from grafana and kibana.

1. Build devlopment envirnament base on container 
  REPOSITORY                                      TAG                 IMAGE ID            CREATED             SIZE
  grafana/grafana                                 latest              a8aa0688fd3a        13 days ago         233MB
  docker.elastic.co/kibana/kibana                 7.6.0               b36db011e72c        4 weeks ago         1.01GB
  kibana                                          7.6.0               b36db011e72c        4 weeks ago         1.01GB
  docker.elastic.co/elasticsearch/elasticsearch   7.6.0               5d2812e0e41c        4 weeks ago         790MB

2. Start Elastic Search,Kibana and Grafana
```
   #!/bin/bash
   basedir=$(cd `dirname $0`;pwd)
   # clear up crash history container
    docker stop grafana
    docker rm grafana
    docker stop es_kibana
    docker rm es_kibana
    docker stop es
    docker rm es
    
    # Start new container
    docker run -d --name es -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" -e "TZ=Asia/Shanghai"  -v "/elk/es/data:/usr/share/elasticsearch/data" -v "/elk/es/logs:/usr/share/elasticsearch/logs" -v "/elk/es/config:/usr/share/elasticsearch/config" docker.elastic.co/elasticsearch/elasticsearch:7.6.0

     docker run --name es_kibana -p 5601:5601 -d -e ELASTICSEARCH_URL=http://127.0.0.1:9200 -e "TZ=Asia/Shanghai" -v "/elk/kibana/data:/usr/share/kibana/data" -v "/elk/kibana/kibana.yml:/usr/share/kibana/config/kibana.yml" docker.elastic.co/kibana/kibana:7.6.0

     docker run -d --name grafana -p 3001:3000 -e "TZ=Asia/Shanghai" -v "/elk/grafana/data:/var/lib/grafana" -v "/elk/grafana/conf:/usr/share/grafana/conf" grafana/grafana:latest
```     
 3. Build go live docker images base docker file and python program
     
     
