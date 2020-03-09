__author__: "Bai Neng"
import logging
from logging import handlers

class Logger(object):
    level_relations = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'crit': logging.CRITICAL
    }

    def __init__(self,filename,level='info',when='D',backCount=3,fmt='%(asctime)s - %(levelname)s: %(message)s'):
        self.logger = logging.getLogger(filename)
        format_str = logging.Formatter(fmt)                   # set data format
        self.logger.setLevel(self.level_relations.get(level)) #set loglevel
        sh = logging.StreamHandler()                          # output to screen
        sh.setFormatter(format_str)                           # set format
        th = handlers.TimedRotatingFileHandler(filename=filename,when=when,backupCount=backCount,encoding='utf-8')#往文件里写入#指定间隔时间自动生成文件的处理器
        # instane for TimedRotatingFileHandler
        # interval，backupCount，when is time units includes：
        # S second
        # M minute
        # H hour
        # D day
        # W week interval==0 express monday
        # midnight 00:00:00
        th.setFormatter(format_str)                            # set format:fmt='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
        self.logger.addHandler(sh)                             # logger
        self.logger.addHandler(th)
            

