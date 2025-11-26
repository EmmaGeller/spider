import time


from urllib import parse, request
import re

from bs4 import BeautifulSoup

from realty_sprider.dbConnect import DBUtils
import logging

#----------------------------------------------------
# table:hezuo
# url:http://ris.szpl.gov.cn/bol/hezuo.aspx?id=*
#----------------------------------------------------

class HeZuo(object):

    def _get_partners_contents(self, url, soup):
        tables = soup.find_all('table', attrs={'bgcolor': '#99ccff'})
        tablelist = []
        for table in tables:
            trslist = []
            for tr in table.find_all('tr'):
                tdlist = []
                for td in tr.find_all('td'):
                    tdlist.append(td.text.strip())
                trslist.append(tdlist)
            tablelist.append(trslist)
        return tablelist

    # 判断是否是所需字段：返回数据库对应字段!
    def _is_need(self, content):
        keydict = {'卖方': 'seller', '地址': 'address', '电话': 'tel',
                   '开发企业资质证书号码': 'qualify_certification', '营业执照号码': 'business_license',
                   '经济类型': 'economic_type', '法定代表人': 'representative', '法人国籍': 'representative_nation',
                   '证件类型': 'certificate_type', '身份证/户照号码:': 're_id_num', '委托代理人': 'principal_agent',
                   '国籍': 'nation', '身份证/护照号码': 'agent_id_num', '电话1': 'agent_phone_num', '地址1': 'agent_address'}
        pattern = re.compile('卖方')
        value = None
        if (content in keydict.keys()):
            value = keydict[content]
        elif (re.match(pattern, content) != None):
            value = keydict['卖方']
        return value

    def _partners_contents(self, url, soup):
        tablelist = self._get_partners_contents(url, soup)
        hezuolist = []
        result = parse.urlparse(url)
        di = parse.parse_qs(result.query)
        presell_id = di['id'][0]
        for k in range(len(tablelist)):
            dict = {}
            dict['presell_id'] = presell_id
            dict['url']=url
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            dict['timestamp'] = timestamp
            for i in range(len(tablelist[k])):
                j = 0;
                while j < len(tablelist[k][i]):
                    key = self._is_need(tablelist[k][i][j])
                    if (key != None and key not in dict):
                        value = tablelist[k][i][j + 1]
                        dict[key] = value
                    elif key in dict:
                        name = tablelist[k][i][j] + '1'
                        key = self._is_need(name)
                        value = tablelist[k][i][j + 1]
                        dict[key] = value
                    j = j + 2
            hezuolist.append(dict)
        return hezuolist,presell_id


    def save_hezuo_tomysql(self, tablename, url, soup):
        buildinglist,presell_id = self._partners_contents(url, soup)
        if(len(buildinglist)!=0):
            qmarks = ','.join(['%s'] * len(buildinglist[0]))  # 用于替换记录值
            cols = ','.join(buildinglist[0].keys())  # 字段名
            sql = "INSERT INTO %s (%s) VALUES (%s)" % (tablename, cols, qmarks)
            values = []
            for building in buildinglist:
                keylist = []
                for key in building.keys():
                    keylist.append(building[key])
                values.append(tuple(keylist))
        else:
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            sql='INSERT INTO hezuo(seller,presell_id,url,timestamp) VALUES(%s,%s,%s,%s)'
            values=[('NULL',presell_id,url,timestamp)]
        return sql, values

        #
        #
        # try:
        #     DBUtils.execute_insertmany(sql, values)
        #     return 'success'
        # except Exception as e:
        #     logging.error('插入数据库出错！！交给上层处理！tablename='+tablename)
        #     raise e


#--------------------------test----------------------------------------------------------------------

# if __name__ == '__main__':
#     # dict = {}
#     # <table width="95%" border="0" align="center" cellpadding="0" cellspacing="1" bgcolor="#99ccff">
#     url = 'http://ris.szpl.gov.cn/bol/hezuo.aspx?id=14068'
#     response = request.urlopen(url)
#     html_cont = response.read().decode("GBK")
#     soup = BeautifulSoup(html_cont, 'lxml')
#     sprider = HeZuo()
#     tablename='hezuo'
#     bi = sprider.save_hezuo_tomysql(tablename,url, soup)
#     print(bi)
