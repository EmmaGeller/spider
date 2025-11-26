

# coding:utf-8
import urllib
from urllib import request
from bs4 import BeautifulSoup
import re

#http://www.bubuko.com/infodetail-1588906.html
def get_result(url):
    request = urllib.request.Request(url)
    request.add_header("Referer","http://ris.szpl.gov.cn/bol/index.aspx")
    request.add_header("User-Agent","Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36")
    reponse = urllib.request.urlopen(request)
    resu = reponse.read().decode('GBK')
    return resu

def get_hiddenvalue(resu):
    # request = urllib.request.Request(url)
    # request.add_header("User-Agent","Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36")
    # reponse = urllib.request.urlopen(request)
    # resu = reponse.read().decode('GBK')
    # resu=get_result(url)
    # <input type="hidden" name="__VIEWSTATE" id="__VIEWSTATE" value="(.*?)" />
    VIEWSTATE = re.findall(r'<input type="hidden" name="__VIEWSTATE" id="__VIEWSTATE" value="(.*?)" />', resu, re.I)
    EVENTVALIDATION = re.findall(r'<input type="hidden" name="__EVENTVALIDATION" id="__EVENTVALIDATION" value="(.*?)" />', resu, re.I)
    return VIEWSTATE[0], EVENTVALIDATION[0]

def get_page(data):
    soup=BeautifulSoup(data,'html.parser')
    page_tag=soup.find('div',class_='PageInfo')
    text=page_tag.get_text()
    print(text)
    totalpage = re.findall(re.compile(pattern=r'总共(.*?)页'), text)[0]
    currentpege=re.findall(re.compile(pattern=r'当前为第(.*?)页'), text)[0]
    return totalpage,currentpege

if __name__=='__main__':
    url = 'http://ris.szpl.gov.cn/bol/index.aspx'
    init_data=get_result(url)
    initpage=1
    data=init_data
    total_page, current_page = get_page(data); currentpage = int(current_page)
    totalpage = int(total_page)
    print(current_page)
