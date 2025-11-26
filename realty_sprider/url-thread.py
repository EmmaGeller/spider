#!/usr/bin/env python
import urllib
from builtins import super
import random
import time
import threading
from bs4 import BeautifulSoup
from realty_sprider.url_sprider import UrlSprider
from selenium import webdriver
import requests
import pymysql
#-----------------------------------
#   crawl_url-thread
#--------------------------------





#中文编码设置
# sys.setdefaultencoding('utf-8')
# Type = sys.getfilesystemencoding()

#------------------------------------------------
# 代理以及tor设置。
session = requests.session()
# session.proxies = {'http':'socks5://127.0.0.1:9050','https':'socks5://127.0.0.1:9050'}

#------------------------------------------------
#   可修改的全局变量参数
HOST, USER, PASSWD, DB, PORT = 'localhost', 'root', 'root', 'emma', 3306 # 数据库连接参数
ROOT_URL='http://ris.szpl.gov.cn/bol/'
insert_sql = "INSERT INTO craw_urls (url,table_type,timestamp,type,page) VALUES (%s,%s,%s,%s,%s)"  #数据存储
THREAD_COUNT =  50  #开启线程数
sql_num_base = 200 #自定义的执行批量插入的随机值基数，当此值为1时则每次获取数据均直接插入。
sql_num_add = 100 #自定义的随机值加数，平均而言，当单独一个线程执行sql_num_base+1/3*sql_num_add次数时执行插入
#   不可修改全局变量参数
#------------------------------------------------
schedule = 0  # 当前线程标志
ErrorList = []
WarnList = []
URLLIST=[]
UrlSprider=UrlSprider()

class Handle_HTML(threading.Thread):
    """docstring for Handle_HTML"""
    def __init__(self, lock, ThreadID, tasklist,Total_TaskNum,currentpege):
        super(Handle_HTML, self).__init__()
        self.lock = lock
        self.ThreadID = ThreadID
        self.tasklist = tasklist
        self.Total_TaskNum = Total_TaskNum
        self.currentpage=currentpege

    def run(self):
        global schedule, ErrorList
        schedule = 0  # 当前线程标志
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
        usag = random.choice(USER_AGENTS)
        connect, cursor = ConnectDB()
        self.lock.acquire()
        print ("The Thread tasklist number :", len(self.tasklist))
        self.lock.release()
        total = len(self.tasklist)
        values = []
        now_requests_num  = 0
        print('tasklist============',self.tasklist)
        for url in self.tasklist:
            # -------------------------
            # 每个请求开始前进行进度说明，对线程上锁
            self.lock.acquire()
            time_Now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            print( "Tread-%s:" % self.ThreadID, time_Now, "Already Completed:[%s] " % (schedule),'remind[%s]:'%(self.Total_TaskNum - schedule))
            self.lock.release()
            # ------------------------
            # 可伪造的头部信息
            headers = {
                    'User-Agent': usag,
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
            now_requests_num += 1
            # -------------------------
            # 请求的具体请求部分
            try:
                # -- 发起
                print('URL start-----',URL)
                time.sleep(random.uniform(0, 1))
                req = urllib.request.Request(url=URL, headers=headers)
                response = urllib.request.urlopen(req)
                result=response.read().decode('GBK')             # --- 请求解析--- 自定义使用正则还是xpath或etree,接口类数据可使用json
                if (result):
                    soup = BeautifulSoup(result, 'lxml')
                    table_type=UrlSprider.get_table_type(URL)
                    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                    #url,table_type,timestamp,type
                    values.append([URL,table_type,timestamp,'0',self.currentpage])# 用于批量插入
                    URLLIST.remove(URL)
                    new_urls=UrlSprider.get_new_urls(ROOT_URL,soup)
                    UrlSprider.add_new_urls(new_urls)
                else:
                    values.append([URL,table_type, timestamp, 'erro',self.currentpage])
            except Exception as e:
                timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                values.append([URL, 'NULL', timestamp, 'erro',self.currentpage])
                print (e)
                time.sleep(random.uniform(0, 3))

            # ------------------------
            # 数据插入部分
            try:
                global sql_num_base
                sql_num = int(random.uniform(sql_num_base, sql_num_base + 100)) #随机一个限制数,200-300 到则进行插入
                if(now_requests_num >= sql_num or now_requests_num>=20):
                    now_requests_num = 0
                    sql_rows=cursor.executemany(insert_sql, values)
                    connect.commit()
                    values = []
                    print('up', time.ctime(), '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&', sql_rows)
            except Exception as e:
                print (e)
                time.sleep(random.uniform(0, 3))
                ErrorList.append("Error:%s\n result:%s" %(e,result))
            # 切换线程
            self.lock.acquire()
            schedule += 1
            self.lock.release()
        try:
            cursor.executemany(insert_sql,values) #防止长时间的请求时间导致数据库连接断开
            connect.commit()
            connect.close()
        except:
            print('erro----insert----------------',)
            cursor.executemany(insert_sql,values)
            connect.commit()
            connect.close()

def ConnectDB():
    "Connect MySQLdb "
    connect,cursor = None, None
    while True:
        try:
            connect = pymysql.connect(
                host=HOST, user=USER, passwd=PASSWD, db=DB, port=PORT, charset='utf8')
            cursor = connect.cursor()
            break
        except pymysql.Error as e:
            print ("Error %d: %s" % (e.args[0], e.args[1]))
            time.sleep(60)#防止出现永远循环
    return connect, cursor


def Thread_Handle(taskList, Total_TaskNum,currentpege):
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
            Work = Handle_HTML(lock, i, source_list, Total_TaskNum,currentpege)
        else:
            source_list = taskList[i * every_thread_number:]
            Work = Handle_HTML(lock, i, source_list, Total_TaskNum,currentpege)
        Work.start()
        WorksThread.append(Work)
    for Work in WorksThread:
        Work.join()

def main(root_url, page, newurls,flag):
    global ErrorList, WarnList,currentpege,totalpage,URLLIST
    try:
        # --------init------------
        chrome_driver_path = "/home/emma/PycharmProjects/sprider/chromedriver"
        driver = webdriver.Chrome(chrome_driver_path)
        driver.get(root_url)
        js = r"__doPostBack('AspNetPager1','%s')" % (page)
        driver.execute_script(js)
        print('page----', page)
        while True:
            print('dont take this to the long way')
            html_cont = driver.page_source
            soup = BeautifulSoup(html_cont, 'lxml')
            currentpege, totalpage = UrlSprider.get_page(soup)
            if ((newurls == None or len(newurls) == 0)and flag!=True):
                urls = UrlSprider.get_new_urls(root_url, soup)
            elif(flag==True):
                urls = newurls
            UrlSprider.add_new_urls(urls)
            print('has new url---------',UrlSprider.has_new_url())
            while(UrlSprider.has_new_url()):
                print('*******************************************')
                rows= UrlSprider.get_urls_set()
                print('rows----------------',len(rows))
                Total_TaskNum=len(rows)
                URLLIST=rows[:]
                Thread_Handle(rows,Total_TaskNum,currentpege)
                print("totalpage-----------", totalpage)
                print('current_page-------------', currentpege)
            if (currentpege == totalpage):
                break
            js = r"__doPostBack('AspNetPager1','%s')" % (currentpege + 1)
            driver.execute_script(js)
            time.sleep(0.3)
            print("_____************_____")
    except Exception as e:
        print('erro*****************************')
        root_url = 'http://ris.szpl.gov.cn/bol/'
        print('>>>>>>>>>>>>>>>>>>>>', currentpege)
        time.sleep(5)
        print('EXCEPTION-------remind----urls----------', URLLIST)
        flag=True
        main(root_url, currentpege, URLLIST,flag)
    if ErrorList:
        for error in ErrorList:
            print(error)
    print("Error:", len(ErrorList), "Warning:", len(WarnList))

if __name__ == '__main__':
    print ("The Program start time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    start = time.time()
    page=2
    newurls=None
    main(ROOT_URL,page,newurls,flag=False)
    print ("The Program end time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "[%s]" % (time.time() - start))



