import re
import time

from bs4 import BeautifulSoup

from realty_sprider.dbConnect import DBUtils
from urllib import parse, request
import logging
#----------------------------------------------------
# house_detail表
#----------------------------------------------------

class HouseDetail(object):
    # 获取许可证详细信息表内容：list
    def _get_housedetail_contents(self, url, soup):
        housedetaillist = []
        trs = soup.find_all('tr', class_='a1')
        for tr in trs:
            tdlist = []
            for td in tr.find_all('td'):
                tdlist.append(td.text.strip())
            housedetaillist.append(tdlist)
        return housedetaillist

    # 判断是否是所需字段：返回数据库对应字段!
    def _is_need(self, content):
        keydict = {'项目楼栋情况': 'building_situation', '座号': 'building_num', '户型': 'house_type',
                   '合同号': 'contract_num', '备案价格': 'record_price', '楼层': 'floor_num', '房号': 'house_num',
                   '用途': 'house_usage', '建筑面积': 'building_area', '户内面积': 'indoor_area', '分摊面积': 'assessed_area',
                   '建筑面积1': 'complete_building_area', '户内面积1': 'complete_indoor_area',
                   '分摊面积1': 'complete_assessed_area'}
        # '用途':'usage','面积':'area','套数':'set_count',
        value = None
        if (content in keydict.keys()):
            value = keydict[content]
        return value

    # 保存许可证详细信息表到dict
    def housedetail_contents(self, url, soup):
        housedetaillist = self._get_housedetail_contents(url, soup)
        result = parse.urlparse(url)
        di = parse.parse_qs(result.query)
        id = di['id'][0]
        housedict = {}
        housedict['id'] = id
        housedict['url']=url
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        housedict['timestamp'] = timestamp
        for i in range(len(housedetaillist)):
            j = 0;
            while j < len(housedetaillist[i]):
                k = housedetaillist[i][j]
                key = self._is_need(k)
                if (key != None and key not in housedict):
                    value = housedetaillist[i][j + 1]
                    housedict[key] = value
                elif (k == '预售查丈' or k == '竣工查丈'):
                    j = j + 1
                    continue
                elif (key in housedict):
                    name = housedetaillist[i][j] + '1'
                    key = self._is_need(name)
                    value = housedetaillist[i][j + 1]
                    housedict[key] = value
                j = j + 2
        return housedict

    # 保存数据到mysql
    def save_housedetail_tomysql(self, tablename, url, soup):
        di = self.housedetail_contents(url, soup)
        qmarks = ','.join(['%s'] * len(di))  # 用于替换记录值
        cols = ','.join(di.keys())  # 字段名
        list = []
        sql = "INSERT INTO %s (%s) VALUES (%s)" % (tablename, cols, qmarks)
        for key in di.keys():
            list.append(di[key])
        return sql,list
        # try:
        #     DBUtils.execute_insert(sql, list)  # 如果字典中某些key的值是空字符串或者NULL值，就会报错
        #     return 'success'
        # except Exception as e:
        #     logging.error('插入数据库出错！！交给上层处理！tablename='+tablename)
        #     raise e

#--------------------------test----------------------------------------------------------------------

if __name__ == '__main__':
    url = 'http://ris.szpl.gov.cn/bol/housedetail.aspx?id=1580579'

    response = request.urlopen(url)
    html_cont = response.read().decode("GBK")
    soup = BeautifulSoup(html_cont, 'lxml')
    s=soup.find('a')
    print(s)
#     HouseDetail = HouseDetail()
#     tablename = 'house_detail'
#     di = HouseDetail.save_housedetail_tomysql(tablename, url, soup)
#     print(di)
