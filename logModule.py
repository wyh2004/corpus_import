# -*- encoding:utf-8 -*-
# 日志处理模块
import logging
import logging.config

logging.config.fileConfig("./logging.conf")

# create logger
logger_name = "example"
logger = logging.getLogger(logger_name)

def gerLogger():
    return logger

