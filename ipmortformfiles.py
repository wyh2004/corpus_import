# -*- coding: utf-8 -*-
import dbModule as dbmodule
import untilModule as untilmodule
import math

# 处理中文乱码
import os

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

# 设置读取语料文件时的缓冲区大小
file_buffer_size = 100
maxbatch = 0
corpus_length = 0
import_success_num = 0
# 从start_index开始导入
start_index = 129000

# 配置日志操作工具
import logModule

logger = logModule.gerLogger()


def file_length(file):
    # 以读方式打开文件，rb为二进制方式(如图片或可执行文件等)
    filehandler = open(file[0], 'r')
    i = 0
    line = filehandler.readline()
    while line:
        i += 1
        line = filehandler.readline()
    filehandler.close()  # 关闭文件句柄
    return i


# 读取语料文件
def readcorpus(file):
    lines = []
    for line in open(file[0]):
        lines.append(line)
    return lines


# 获取语言UUID
def getlantype(file):
    con = dbmodule.dbconnect()
    cur = con.cursor()
    cur.prepare('select uuid,shortname from lan_language where shortname = :shortname')
    cur.execute(None, {'shortname': file[1]})
    res = cur.fetchone()
    cur.close()
    con.close()
    return res


# 获取最大批次
def get_maxbatch():
    sqlstring = 'select decode(max(importbatch),null,0,max(importbatch))+1 maxbatch  from cor_corpus'
    return int(dbmodule.querydb(sqlstring)[0][0])


# 将数据插入数据库
def insert_2_db(content_list_2_db):
    global import_success_num
    con = dbmodule.dbconnect()
    cur = con.cursor()
    cur.executemany("insert into cor_corpus\n" +
                    "  (uuid, groupuuid, content, lanuuid, md5, importtime, importbatch)\n" +
                    "values\n" +
                    "  (:1,:2,:3,:4,:5,sysdate,:6)", content_list_2_db)
    con.commit()
    import_success_num += 1
    cur.close()
    con.close()


def get_groupuuid_by_md5(md5_check):
    sqlstr = "select groupuuid,md5 from cor_corpus where md5 = '" + md5_check + "'"
    resultset = dbmodule.querydb(sqlstr)
    if (resultset):
        return resultset[0][0]
    else:
        return None
        # return dbmodule.querydb(sqlstr)


# 检查句对的md5值，只有当句对在语料中出现过才放回false
def contentpair_md5_check(md5_check_list):
    groupuuid_from_md5 = map(get_groupuuid_by_md5, md5_check_list)
    if len(set(groupuuid_from_md5)) == 1 and set(groupuuid_from_md5).pop() == None:
        return True
    else:
        return False
    return False


# 保存数据
def save_2_db(corpus_pair_list, lantype):
    global index, file_buffer_size, corpus_length
    logger.info("正在保存第%d组数据，共%d组。", index / file_buffer_size, math.ceil(float(corpus_length) / float(file_buffer_size)))
    for corpus_pair in corpus_pair_list:
        corpus_pair_uuid = untilmodule.uuidgenerater()
        # 按对对数据进行处理
        content_pair_2_db = []
        for content in corpus_pair:
            contentindex = corpus_pair.index(content)
            content = content.replace("\n", "")
            content_md5 = untilmodule.md5generater(content)
            content_2_db = []
            content_2_db.append(untilmodule.uuidgenerater())
            content_2_db.append(corpus_pair_uuid)
            content_2_db.append(content)
            content_2_db.append(lantype[contentindex][0])
            content_2_db.append(content_md5)
            content_2_db.append(maxbatch)
            content_pair_2_db.append(tuple(content_2_db))

        # 检查md5值是否已存在，已存在退出该组数据的导入
        # 语言间的md5值也可能重复
        md5_check_list = []
        for content in content_pair_2_db:
            md5_check_list.append(content[4])

        if contentpair_md5_check(md5_check_list):
            insert_2_db(content_pair_2_db)
        else:
            logger.debug("句对已存在，未导入！")
            for sentents in content_pair_2_db:
                logger.debug(sentents[2])


def import_corpus(filelist, filelength):
    #     导入语料
    # 获取文件句柄列表
    global import_success_num, index
    filehandle_list = []
    for file in filelist:
        filehandle_list.append(open(file[0], 'r'))

    # 获取语言类型uuid
    lantype = map(getlantype, filelist)

    # 循环读取文件
    index = 1
    corpus_pair_list = []
    while index <= filelength :
        content_pair = []
        for filehandle in filehandle_list:
            content = filehandle.readline()
            content_pair.append(content)
        corpus_pair_list.append(content_pair)
        # 以file_buffer_size为单位提交数据库保存
        if index % file_buffer_size == 0 or index == filelength:
            if index>start_index:
                save_2_db(corpus_pair_list, lantype)
                corpus_pair_list = []
            else:
                corpus_pair_list = []
            logger.info("已完成 %d 行数据处理,成功导入%d个句对。",index,import_success_num)
        index += 1

    # 关闭打开的文件
    for filehandle in filehandle_list:
        filehandle.close()


if __name__ == "__main__":
    # 设置语料文件
    filelist = []
    filelist.append(["data/train.en", "en"])
    filelist.append(["data/train.vi", "vi"])
    filelist.append(["data/train.zh.trans", "zh"])

    # 获取文件长度列表
    filelength = map(file_length, filelist)
    corpus_length = max(filelength)
    logger.info("共有%d行语料，开始导入！", corpus_length)
    # 文件长度相等，且行数大于1，导入语料
    if max(filelength) == min(filelength) and min(filelength) <> 0:
        lantype = map(getlantype, filelist)
        maxbatch = get_maxbatch()
        #     导入语料
        #     import_corpus(filelist,max(filelength))
        import_corpus(filelist, max(filelength))
    else:
        logger.error("文件长度不一致，请检查数据后重试")
        logger.error(filelength)
        exit()
    logger.info("程序结束,导入批次: %d 。",maxbatch)
