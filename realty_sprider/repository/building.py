import re
from bs4 import BeautifulSoup
from urllib import request
from realty_sprider.dbConnect import DBUtils
import logging
import time
#----------------------------------------------------
# building表
# url:http://ris.szpl.gov.cn/bol/building.aspx?id=*&presellid=*
#----------------------------------------------------

class Building(object):
    def _get_building_contents(self,url,soup):
        trs = soup.find_all('tr', class_='a1')
        status_tag = soup.find('td', class_='isblockH')
        building_num_tag = soup.find('div', id='divShowBranch').font
        resu = url + '/'
        id = re.findall(r'http://ris.szpl.gov.cn/bol/building.aspx\?id=(.*?)&presellid=(.*?)[&^\s]*/', resu)
        presellid = re.findall('^[1-9]\d*', id[0][1])
        building_id = id[0][0]
        s = soup.find('div', id='divShowBranch').parent.parent
        time_tag = s.find_all('td')[2]
        update_time=time_tag.text[5:]
        status = status_tag.text.strip()
        if(  building_num_tag==None):
            return None
        building_num = building_num_tag.text.strip()
        floor_num = None
        trlist = []
        for tr in trs:
            for td in tr.find_all('td'):
                dict = {}
                for div in td.find_all('div'):
                    if (re.match(re.compile(r'\d.*层$'), div.text) != None and 'floor_num' not in dict):
                        floor_num = div.text
                        dict['floor_num '] = div.text
                    if (re.match('房号：', div.text) != None):
                        dict['house_num'] = div.text[3:]
                    if (div.a != None):
                        resu = div.a['href'] + '/'
                        house_id = re.findall(r'housedetail.aspx\?id=(.*?)/', resu)
                        if (len(house_id)!=0):
                            dict['house_id'] = house_id[0]
                        if (div.a.img != None):
                            house_status = re.findall(r'imc/(.*?).gif', div.a.img['src'])
                            dict['house_status'] = house_status[0]
                if (floor_num != None and 'house_id' in dict):
                    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                    dict['timestamp']=timestamp
                    dict['floor_num '] = floor_num
                    dict['status'] = status
                    dict['building_num'] = building_num
                    dict['update_time']=update_time
                    dict['building_id']=building_id
                    dict['presell_id']=presellid
                    dict['url']=url
                    trlist.append(dict)
        return trlist

    # 保存数据到mysql
    def save_building_tomysql(self,tablename,url,soup):
        buildinglist=self._get_building_contents(url,soup)
        if buildinglist==None:
            return None
        if(len(buildinglist)!=0):
            qmarks = ','.join(['%s'] * len(buildinglist[0]))  # 用于替换记录值
            cols = ','.join(buildinglist[0].keys())  # 字段名
            sql = "INSERT INTO %s (%s) VALUES (%s)" % (tablename, cols, qmarks)
            values=[]
            for building in buildinglist:
                keylist = []
                for key in building.keys():
                    keylist.append(building[key])
                values.append(tuple(keylist))
            # return sql,values
        else:
            return None;
        try:
            DBUtils.execute_insertmany(sql,values)  # 如果字典中某些key的值是空字符串或者NULL值，就会报错
            return 'success'
        except Exception as e:
            logging.error('插入数据库出错！！交给上层处理！tablename='+tablename)
            raise e

# --------------------------test----------------------------------------------------------------------

if __name__ == '__main__':
    url = 'http://ris.szpl.gov.cn/bol/building.aspx?id=7692&presellid=1800&Branch=3%b5%a5%d4%aa&isBlock=ys'
    response = request.urlopen(url)
    html_cont = response.read().decode("GBK")
    soup = BeautifulSoup(html_cont, 'lxml')
    build = Building()
    tablename = 'building'
    buildinglist = build.save_building_tomysql(tablename, url, soup)
    print(buildinglist)



