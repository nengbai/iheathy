pipeline {
    agent none 
    stages {
        stage('Build') { 
            agent {
                docker {
                    image 'python:2-alpine' 
                }
            }
            steps {
                sh 'python -m py_compile sina_cash_import_to_es.py sina_cash_es.py' 
                stash(name: 'compiled-results', includes: '*.py*') 
            }
        }
    }
}
