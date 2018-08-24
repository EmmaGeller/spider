import time
import urllib
from bs4 import BeautifulSoup
import re
from selenium import webdriver
from realty_sprider import url_manager, html_downloader, html_parser, MyExceptionParse
import logging


class SpriderMain(object):
    def __init__(self):
        self.urls = url_manager.UrlManager()
        self.downloader = html_downloader.HtmlDownloader()
        self.parser = html_parser.HtmlParser()
        self.except_paser = MyExceptionParse.ExceptionParse()

    def craw(self,newurls):
        try:
            count = 1
            self.urls.add_new_urls(newurls)
            while self.urls.has_new_url():
                new_url = self.urls.get_new_url()
                print('craw ------ %d:%s' % (count, new_url))
                html_cont = self.downloader.download(new_url)
                new_urls, new_data = self.parser.parser(new_url, html_cont)
                self.urls.add_new_urls(new_urls)
                self.urls.save_url()
                time.sleep(2)
                count = count + 1
        except Exception as e:
            raise e
        return True

    def _get_page_urls(self, root_url, soup):
        new_urls = set()
        links = soup.find_all('a', href=re.compile(r"\w+\.aspx"))
        for link in links:
            new_url = link['href']
            new_full_url = urllib.parse.urljoin(root_url,new_url)
            new_urls.add(new_full_url)
        return new_urls


    def page_loader(self, root_url, page, newurls):
        global currentpege
        try:
            chrome_driver_path = "/home/emma/文档/chromedriver_linux64/chromedriver"
            driver = webdriver.Chrome(chrome_driver_path)
            driver.get(root_url)
            js = r"__doPostBack('AspNetPager1','%s')" % (page)
            driver.execute_script(js)
            print('page----',page)
            while (True):
                html_cont = driver.page_source
                soup = BeautifulSoup(html_cont, 'lxml')
                currentpege, totalpage =self.parser.get_page(soup)
                if(newurls==None or len(newurls)==0):
                    self.parser.proccessing_projects(soup)
                    urls = self._get_page_urls(root_url, soup)
                else:
                    urls=newurls
                    print('**********',newurls)
                print("totalpage-----------", totalpage)
                print('current_page-------------', currentpege)
                state = self.craw(urls)
                if (currentpege == totalpage):
                    break
                if (state == True):
                    js = r"__doPostBack('AspNetPager1','%s')" % (currentpege+1)
                    driver.execute_script(js)
                    time.sleep(5)
        except Exception as e:
            print('*****************************')
            root_url = 'http://ris.szpl.gov.cn/bol/'
            remind_urls = self.urls.get_urls_set()
            old_urls = self.urls.get_old_urls()
            erro_url=self.urls.get_current_url()
            current_page=currentpege
            print('>>>>>>>>>>>>>>>>>>>>',current_page)
            logging.error("erro_url----",erro_url)
            self.except_paser.save_exception_urls(old_urls,remind_urls,erro_url,current_page)
            newurls=self.except_paser.handling_errors()
            time.sleep(5)
            self.page_loader(root_url,current_page,newurls)


if __name__ == '__main__':
    SpriderMain = SpriderMain()
    root_url = 'http://ris.szpl.gov.cn/bol/'
    page=3
    newurls=None
    SpriderMain.page_loader(root_url,page,newurls)

