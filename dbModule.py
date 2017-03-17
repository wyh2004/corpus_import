# -*- coding: utf-8 -*-
# 数据库处理模块
import cx_Oracle

def dbconnect():
    con = cx_Oracle.connect('username/username@127.0.0.1/corpus')
    return con

def querydb(sqlstring):
    con = dbconnect()
    cur = con.cursor()
    cur.execute(sqlstring)
    res = cur.fetchall()
    cur.close()
    con.close()
    return res

