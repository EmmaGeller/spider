import time

import pymysql
import logging
from realty_sprider.dbConnect import db_config

__host = db_config.getConfig("database", "dbhost")
__user = db_config.getConfig("database", "dbuser")
__passwd = db_config.getConfig("database", "dbpassword")
__db = db_config.getConfig("database", "dbname")
__port = int(db_config.getConfig("database", "dbport"))
__charset = db_config.getConfig("database", "dbcharset")

def getConnect():
    try:
        conn = pymysql.connect(host=__host, user=__user, passwd=__passwd, db=__db,
                               port=__port, charset=__charset)
        return conn
    except pymysql.Error as e:
        print("Mysqldb Error:%s" % e)


def execute(sql_str):
    if sql_str is None:
        raise Exception("参数不能为空：sql_str")
    if len(sql_str) == 0:
        raise Exception("参数不能为空：sql_str")
    try:
        conn = getConnect()
        cur = conn.cursor()
        cur.execute(sql_str)
        data = cur.fetchall()
        conn.commit()
        return data
    except Exception as e:
        raise e
    finally:
        cur.close()
        conn.close()


# 插入数据，返回数据主键
def execute_insert(insert_str, data):
    if insert_str is None:
        raise Exception("参数不能为空：sql_str")
    if len(insert_str) == 0:
        raise Exception("参数不能为空：sql_str")
    try:
        conn = getConnect()
        cur = conn.cursor()  # 获取一个游标
        cur.execute(insert_str, data)
        fet = cur.fetchall()
        last_id = conn.insert_id()
        conn.commit()
        return last_id
    except pymysql.Error as e:
        print ("Error1 %d: %s" % (e.args[0], e.args[1]))
        time.sleep(60)
        conn.rollback()
        logging.error('插入数据库出错！！交给上层处理！!')
        raise e
    finally:
        cur.close()
        conn.close()


# 插入数据，返回数据主键
def execute_insertmany(insert_str, data):
    if insert_str is None:
        raise Exception("参数不能为空：sql_str")
    if len(insert_str) == 0:
        raise Exception("参数不能为空：sql_str")
    try:
        conn = getConnect()
        cur = conn.cursor()
        cur.executemany(insert_str, data)
        data = cur.fetchall()
        last_id = conn.insert_id()
        conn.commit()
        return last_id
    except pymysql.Error as e:
        print ("Error1 %d: %s" % (e.args[0], e.args[1]))
        time.sleep(60)
        conn.rollback()
        logging.error('插入数据库出错！！交给上层处理！!')
        raise e
    finally:
        cur.close()
        conn.close()


def execute_batch_sql(*list):
    if list is None:
        raise Exception("参数不能为空：sql_str")
    if len(list) == 0:
        raise Exception("参数不能为空：sql_str")
    try:
        conn = getConnect()
        cur = conn.cursor()
        i = 0
        while i < len(list):
            cur.executemany(list[i],list[i + 1])
            i = i + 1
        data = cur.fetchall()
        last_id = conn.insert_id()
        conn.commit()
        return last_id
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()


# 更新数据，返回更新条数
def execute_update(update_str, data):
    if update_str is None:
        raise Exception("参数不能为空：update_str")
    if len(update_str) == 0:
        raise Exception("参数不能为空：update_str")
    try:
        conn = getConnect()
        cur = conn.cursor()
        count = cur.execute(update_str, data)
        conn.commit()
        return count
    except Exception as e:
        logging.error('插入数据库出错！！交给上层处理！')
        raise e
    finally:
        cur.close()
        conn.close()


# 执行带参数的查询，返回查询结果
def execute_select(select_str, data):
    if select_str is None:
        raise Exception("参数不能为空：sql_str")
    if len(select_str) == 0:
        raise Exception("参数不能为空：sql_str")
    try:
        conn = getConnect()
        cur = conn.cursor()  # 获取一个游标
        cur.execute(select_str, data)
        data = cur.fetchall()
        conn.commit()
        return data
    except Exception as e:
        logging.error('插入数据库出错！！交给上层处理！!')
        raise e
    finally:
        cur.close()
        conn.close()


# 执行带参数的删除
def execute_delete(select_str, data):
    if select_str is None:
        raise Exception("参数不能为空：sql_str")
    if len(select_str) == 0:
        raise Exception("参数不能为空：sql_str")
    try:
        conn = getConnect()
        cur = conn.cursor()  # 获取一个游标
        cur.execute(select_str, data)
        data = cur.fetchall()
        conn.commit()
        return data
    except Exception as e:
        raise e
    finally:
        cur.close()
        conn.close()
