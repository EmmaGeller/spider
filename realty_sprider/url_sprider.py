import urllib
from bs4 import BeautifulSoup
from selenium import webdriver
from realty_sprider.dbConnect import DBUtils
import time
import re
import logging
from realty_sprider import  html_downloader, html_parser, MyExceptionParse

class UrlSprider(object):
    def __init__(self):
        self.downloader = html_downloader.HtmlDownloader()
        self.except_paser = MyExceptionParse.ExceptionParse()
        self.parser = html_parser.HtmlParser()
        self.new_urls = []
        self.old_urls = []
        self.url = ''

    def add_new_url(self, url):
        if url is None:
            return
        if url not in self.new_urls and url not in self.old_urls:
            self.new_urls.append(url)

    def add_new_urls(self, urls):
        if urls is None or len(urls) == 0:
            return
        for url in urls:
            self.add_new_url(url)

    def has_new_url(self):
        return len(self.new_urls) != 0

    def get_new_url(self):
        new_url = self.new_urls.pop()
        self.old_urls.append(new_url)
        return new_url

    def get_old_urls(self):
        return self.old_urls

    def get_urls_set(self):
        newurls=[]
        count=1
        if len(self.new_urls)<3000:
            while self.has_new_url():
                newurls.append(self.get_new_url())
        else:
            while count<=3000:
                newurls.append(self.get_new_url())
                count=count+1
        return newurls

    def get_current_url(self):
        return self.url

    def get_new_urls(self, page_url, soup):
        if page_url is None :
            return None
        # soup = BeautifulSoup(html_cont, 'lxml')
        new_url_set = []
        links = soup.find_all('a', href=re.compile(r"\w+\.aspx"))
        for link in links:
            new_url = link['href']
            new_full_url = urllib.parse.urljoin(page_url, new_url)
            new_url_set.append(new_full_url)
        return new_url_set

    '''1-certdetail 2-projectdetail 3-hezuo 4-building 5-house_detail 6-project-items'''

    def get_table_type(self, url):
        table_type = re.findall(r'http://ris.szpl.gov.cn/bol/(.*?)\.aspx\?', url)
        if (table_type != None):
            return self._get_type(table_type[0])
        elif(url=='http://ris.szpl.gov.cn/bol/'):
            return '6'

    def _get_type(self,table_type):
        return {
            'certdetail': '1',
            'projectdetail': '2',
            'hezuo': '3',
            'building': '4',
            'housedetail': '5'
        }.get(table_type)

    def save_new_urls(self,values):
        try:
            insert_sql = 'INSERT INTO craw_urls (url,table_type,timestamp,type) VALUES (%s,%s,%s,%s)'
            DBUtils.execute_inser(insert_sql, values)
        except Exception as e:
            logging.error('插入数据库出错！！！！')
        return 'suceess'

    def get_page(self,soup):
        page_tag = soup.find('div', class_='PageInfo')
        text = page_tag.get_text()
        totalpage = re.findall(re.compile(pattern=r'总共(.*?)页'), text)[0]
        currentpege = re.findall(re.compile(pattern=r'当前为第(.*?)页'), text)[0]
        return currentpege,totalpage

    # def craw(self, root_url):
    #     try:
    #         count = 1
    #         self.add_new_urls(root_url)
    #         while self.has_new_url():
    #             new_url = self.get_new_url()
    #             print('craw ------ %d:%s' % (count, new_url))
    #             html_cont = self.downloader.download(new_url)
    #             new_url_set = self.get_new_urls(new_url, html_cont)
    #             self.add_new_urls(new_url_set)
    #             self.save_new_urls(new_url)
    #             count = count + 1
    #         print(len(self.old_urls))
    #         print(count)
    #     except Exception as e:
    #         raise e
    #     return True
    #
    # def page_loader(self, root_url, page, newurls):
    #     global currentpege
    #     try:
    #         chrome_driver_path = "/home/emma/文档/chromedriver_linux64/chromedriver"
    #         driver = webdriver.Chrome(chrome_driver_path)
    #         driver.get(root_url)
    #         js = r"__doPostBack('AspNetPager1','%s')" % (page)
    #         driver.execute_script(js)
    #         print('page----', page)
    #         while (True):
    #             html_cont = driver.page_source
    #             soup = BeautifulSoup(html_cont, 'lxml')
    #             currentpege, totalpage = self.get_page(soup)
    #             if (newurls == None or len(newurls) == 0):
    #                 urls = self.get_new_urls(root_url, html_cont)
    #             else:
    #                 urls = newurls
    #                 print('**********', newurls)
    #             print("totalpage-----------", totalpage)
    #             print('current_page-------------', currentpege)
    #             state = self.craw(urls)
    #             if (currentpege == totalpage):
    #                 break
    #             if (state == True):
    #                 js = r"__doPostBack('AspNetPager1','%s')" % (currentpege + 1)
    #                 driver.execute_script(js)
    #                 time.sleep(2)
    #     except Exception as e:
    #         print('*****************************')
    #         root_url = 'http://ris.szpl.gov.cn/bol/'
    #         erro_url = self.get_current_url()
    #         current_page = currentpege
    #         print('>>>>>>>>>>>>>>>>>>>>', current_page)
    #         logging.error("erro_url----", erro_url)
    #         self.except_paser.save_exception_urls(erro_url, current_page)
    #         newurls = self.except_paser.handling_errors()
    #         time.sleep(5)
    #         self.page_loader(root_url, current_page, newurls)