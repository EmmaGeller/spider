

import random
import time
import pymysql
import threading
import requests
from bs4 import BeautifulSoup
from realty_sprider.repository import building, certdetail, hezuo, housedetail, projectdetail,presell_project

#--------------------------------------------------
# 中文编码设置
# reload(sys)
# sys.setdefaultencoding('utf-8')
# Type = sys.getfilesystemencoding()

#------------------------------------------------
# 代理以及tor设置。
session = requests.session()
# session.proxies = {'http':'socks5://127.0.0.1:9050','https':'socks5://127.0.0.1:9050'}

#------------------------------------------------
#   可修改的全局变量参数
Table = "craw_urls"  # 表名称需修改
HOST, USER, PASSWD, DB, PORT = '192.168.2.172', 'root', 'root', 'emma', 3306  # 数据库连接参数
# 在数据库中i已经打乱了.
select_sql = "SELECT id,url,table_type FROM %s where  table_type='5' and type='0' limit 3000;"
Update_sql = "UPDATE " + Table + " SET type=%s WHERE id =%s;"  # 数据存储

THREAD_COUNT = 150  # 开启线程数
sql_num_base = 200  # 自定义的执行批量插入的随机值基数，当此值为1时则每次获取数据均直接插入。
sql_num_add = 1000  # 自定义的随机值加数，平均而言，当单独一个线程执行sql_num_base+1/3*sql_num_add次数时执行插入
#   不可修改全局变量参数
#------------------------------------------------
schedule = 0  # 当前线程标志
ErrorList = []
WarnList = []
sleeptime=random.randint(0, 3)

class Handle_HTML(threading.Thread):
    """docstring for Handle_HTML"""
    def __init__(self, lock, ThreadID, tasklist, Total_TaskNum):
        super(Handle_HTML, self).__init__()
        self.lock = lock
        self.ThreadID = ThreadID
        self.tasklist = tasklist
        self.Total_TaskNum = Total_TaskNum
        self.building = building.Building()
        self.certdetail = certdetail.CertDetail()
        self.hezuo = hezuo.HeZuo()
        self.housedetail = housedetail.HouseDetail()
        self.projectdetail = projectdetail.ProjectDetail()
        self.prsellProjects=presell_project.PresellProject()

    def run(self):
        global schedule, ErrorList
        USER_AGENTS = [
            "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
            "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
            "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
            "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
            "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
            "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
            ]
        connect, cursor = ConnectDB()
        self.lock.acquire()
        print ("The Thread tasklist number :", len(self.tasklist))
        self.lock.release()
        total = int(len(self.tasklist))
        count=0;
        for (id,url,table_type) in self.tasklist:
            # -------------------------
            # 每个请求开始前进行进度说明，对线程上锁
            self.lock.acquire()
            time_Now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            self.lock.release()
            # ------------------------
            # 可伪造的头部信息
            user_Agent = random.choice(USER_AGENTS)
            headers = {
                    'User-Agent': user_Agent,
                    'Referer':'',
                    'X-Forwarded-For': 'http://dev.qkgame.com.cn:9802/random',
                    'Accept':'*/*',
                    'Accept-Encoding':'gzip, deflate, sdch',
                    'Accept-Language':'zh-CN,zh;q=0.8',
                    'Cache-Control':'no-cache',
                    'Connection':'keep-alive',
                    # 'Host':'ditu.amap.com',
                    'Pragma':'no-cache',
                    'Referer':''
                    #User-Agent:Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/53.0.2785.143 Chrome/53.0.2785.143 Safari/537.36
                    }
            URL = url
            count+=1;
            # print '*************************************',ip,i#,date_list
            # -------------------------
            # 请求的具体请求部分
            try:
                # -- 发起
                time.sleep(sleeptime)
                result = session.get(URL, headers=headers)
                result = result.content.decode('GBK','ignore')
                if(result):
                    # match:从字符串的起始位置匹配，如果起始位置匹配不成功的话，match()就返回none
                        try:
                            print('table-----type====',table_type)
                            print('url================',url)
                            soup = BeautifulSoup(result, 'lxml')
                            #'''1-certdetail 2-projectdetail 3-hezuo 4-building 5-house_detail 6-project-items'''
                            # if (table_type=='1'):
                            #     # tablename = 'certdetail'
                            #     sql, list, usage_sql, usage_values = self.certdetail.save_certdetail_tomysql(URL, soup)
                            #     cursor.execute(sql, list)
                            #     cursor.executemany(usage_sql, usage_values)
                            #     print('certdetail___usagelist-----------')
                            # if (table_type=='2'):
                            #     tablename = 'projectdetail'
                            #     sql, projectdetails, regs_sql, reg_values, infosql, project_buildings = self.projectdetail.save_projectdetail_tomysql(tablename, URL, soup)
                            #     cursor.execute(sql, projectdetails)
                            #     cursor.executemany(regs_sql,reg_values)
                            #     cursor.executemany(infosql, project_buildings)
                            #     print("projectdetail--------------------------------")
                            # if (table_type=='3'):
                            #     tablename = 'hezuo'
                            #     if(hezuo.save_hezuo_tomysql(tablename, URL, soup)==None):
                            #         print('')
                            #     else:
                            #         sql, values = self.hezuo.save_hezuo_tomysql(tablename, URL, soup)
                            #         cursor.executemany(sql,values)
                            #
                            #         print("hezuo------------------")
                            # if (table_type=='4'):
                            #     tablename = 'building'
                            #     if(self.building.save_building_tomysql(tablename, URL, soup)==None):
                            #         print("building--------NULL-------------")
                            #         pass
                            #     else:
                            #         sql, values = self.building.save_building_tomysql(tablename, URL, soup)
                            #         cursor.executemany(sql, values)
                            #         print("building---------------------")


                            if (table_type=='5'):
                                tablename = 'house_detail'
                                sql,list = self.housedetail.save_housedetail_tomysql(tablename, URL, soup)
                                cursor.execute(sql, list)
                                print("housedetails----------------------")
                            # elif (table_type=='6'):
                            #     sql, projects=self.prsellProjects.save_presellproject_tomysql(URL,soup)
                            #     cursor.executemany(sql, projects)
                            else:
                                print('其他url：', URL)

                            cursor.execute(Update_sql, (1, id))
                            connect.commit()

                        except Exception as e:
                            print('EXception1----------------------------------')
                            connect.rollback()
                            cursor.execute(Update_sql, (str(e), id))
                            connect.commit()
                            time.sleep(random.uniform(0, 3))
                            print(e)
                # else:
                #     cursor.execute(Update_sql,(0,id))
                #     connect.commit()
            except AssertionError:
                print('5')
            except Exception as e:
                print('EXception2----------------------------------')
                cursor.execute(Update_sql,(str(e),id))
                connect.commit()
                print (e)
                # print('4')
                time.sleep(random.uniform(0, 3))
                ErrorList.append(
                    "The ip is :Error:%s\n result:%s" % ( e, result))
                continue
            # 切换线程
            self.lock.acquire()
            schedule += 1
            self.lock.release()
        connect.close()

def ConnectDB():
    "Connect MySQLdb "
    connect, cursor = None, None
    while True:
        try:
            connect = pymysql.connect(
                host=HOST, user=USER, passwd=PASSWD, db=DB, port=PORT, charset='utf8')
            cursor = connect.cursor()
            break
        except pymysql.Error as e:
            print ("Error1 %d: %s" % (e.args[0], e.args[1]))
            time.sleep(60)#防止出现永远循环
    return connect, cursor


def Thread_Handle(taskList, Total_TaskNum):
    '''多线程启动区域--无需修改'''
    global THREAD_COUNT
    lock = threading.Lock()
    WorksThread = []
    every_thread_number = len(taskList) // THREAD_COUNT
    if every_thread_number == 0:
        THREAD_COUNT = len(taskList)
        every_thread_number = 1

    for i in range(THREAD_COUNT):
        if i != THREAD_COUNT - 1:
            source_list = taskList[
                i * every_thread_number: (i + 1) * every_thread_number]
            Work = Handle_HTML(lock, i, source_list, Total_TaskNum)
        else:
            source_list = taskList[i * every_thread_number:]
            Work = Handle_HTML(lock, i, source_list, Total_TaskNum)
        Work.start()
        WorksThread.append(Work)
    for Work in WorksThread:
        Work.join()


def main():
    global ErrorList, WarnList
    connect, cursor = ConnectDB()
    # 统计表总行数,依据flag = 3
    try:
        cursor.execute("SELECT COUNT(*) FROM %s WHERE type=0 ;" % Table)
    except Exception as e:
        print (e+'2')
    TaskNum = cursor.fetchall()
    connect.close()

    if TaskNum[0][0] == 0:
        print ("Warning:There is no need to do the task!!!")
    else:
        Total_TaskNum = int(TaskNum[0][0])
        while True:
            connect,cursor = ConnectDB()  # 建立数据库连接
            try:
                if cursor.execute(select_sql % Table):  # 取任务url
                    rows = cursor.fetchall()
                    Thread_Handle(rows, Total_TaskNum)  # 线程启动
                else:
                    break
            except Exception as e:
                print (e+'1')
            connect.close()
    print ("_____************_____")
    if ErrorList:
        for error in ErrorList:
            print (error)
    print ("Error:", len(ErrorList), "Warning:", len(WarnList))

if __name__ == '__main__':
    print ("The Program start time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    start = time.time()
    main()
    print ("The Program end time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "[%s]" % (time.time() - start))
    # raw_input("Please enter any key to exit!")
