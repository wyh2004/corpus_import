# -*- coding: utf-8 -*-
# 基本工具模块


# MD5校验
import hashlib
# uuid生成
import uuid

#    去除标点和空格
def remove_punctuation(mystring):
    import string
    out = mystring.translate(string.maketrans("", ""), string.punctuation + " ")
    return out

#   生成md5值
def md5generater(mystring):
    newstr = remove_punctuation(mystring)
    m2 = hashlib.md5()
    m2.update(newstr)
    return m2.hexdigest()

#   生成uuid
def uuidgenerater():
    return str(uuid.uuid1()).replace("-", "")