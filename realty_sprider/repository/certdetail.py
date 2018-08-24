import re
import time
from urllib import parse
import logging

import pymysql

from realty_sprider.dbConnect import DBUtils

#----------------------------------------------------
# certdetail表------using_detail
#----------------------------------------------------

class CertDetail(object):
    # 获取许可证详细信息表内容：list
    def _get_certdetail_contents(self, url, soup):
        certdetaillist = []
        trs = soup.find_all('tr', class_='a1')
        for tr in trs:
            tdlist = []
            for td in tr.find_all('td'):
                tdlist.append(td.text.strip())
            certdetaillist.append(tdlist)
        return certdetaillist

    def _is_usage(self, content):
        keydict = {'用途': 'house_usage', '面积': 'area', '套数': 'set_count'}
        value = None
        if (content in keydict.keys()):
            value = keydict[content]
        return value

    # 判断是否是所需字段：返回数据库对应字段!
    def _is_need(self, content):
        keydict = {'许可证号': 'cert_num', '项目名称': 'project_name', '发展商': 'developers',
                   '所在位置': 'location', '栋数': 'building_count', '地块编号': 'plot_num', '房产证编号': 'property_certify',
                   '批准面积': 'approved_area', '土地出让合同': 'land_transfer_contract', '批准日期': 'approved_date',
                   '发证日期（开始销售日期）': 'licence_date', '备注': 'note'}
        # '用途':'usage','面积':'area','套数':'set_count',
        value = None
        if (content in keydict.keys()):
            value = keydict[content]
        return value

    # 保存许可证详细信息表到dict
    def _certdetail_contents(self, url, soup):
        certdetaillist = self._get_certdetail_contents(url, soup)
        dict = {}
        result = parse.urlparse(url)
        usagelist = []
        di = parse.parse_qs(result.query)
        presell_id = di['id'][0]
        dict['presell_id'] = presell_id
        dict['url']=url
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        dict['timestamp']=timestamp
        for i in range(len(certdetaillist)):
            j = 0;
            usagedict = {};
            usagedict['presell_id'] = presell_id
            usagedict['url']=url
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            usagedict['timestamp'] = timestamp
            while j < len(certdetaillist[i]):
                k = certdetaillist[i][j]
                key = self._is_need(k)
                if (key != None):
                    value = certdetaillist[i][j + 1]
                    dict[key] = value
                elif (self._is_usage(k) != None):
                    usagekey = self._is_usage(k)
                    value = certdetaillist[i][j + 1]
                    usagedict[usagekey] = value;
                    if (self._is_usage(k) == 'set_count' and len(usagedict) != 0):
                        usagelist.append(usagedict)
                j = j + 2
        return dict, usagelist

    # 保存数据到mysql
    def save_certdetail_tomysql(self,url, soup):
        tablename = 'certdetail'
        di, usagelist = self._certdetail_contents(url, soup)
        qmarks = ','.join(['%s'] * len(di))  # 用于替换记录值
        cols = ','.join(di.keys())  # 字段名
        list = []
        sql = "INSERT INTO %s (%s) VALUES (%s)" % (tablename, cols, qmarks)
        for key in di.keys():
            list.append(di[key])
        usage_sql, usage_values = self._save_usagelist(usagelist)
        return sql,list,usage_sql,usage_values
        # try:
        #     cur.execute(sql,list)
        #     cur.executemany(usage_sql, usage_values)
        #     data = cur.fetchall()
        #     conn.commit()
        #     return 'success'
        # except pymysql.Error as e:
        #     print ("Error1 %d: %s" % (e.args[0], e.args[1]))
        #     time.sleep(60)
        #     conn.rollback()
        #     logging.error('插入数据库出错！！tablename=' + tablename)
        #     raise e
        # finally:
        #     cur.close()
        #     conn.close()

    def _save_usagelist(self, di):
        qmarks = ','.join(['%s'] * len(di[0]))  # 用于替换记录值
        cols = ','.join(di[0].keys())  # 字段名
        tablename = 'using_detail'
        sql = "INSERT INTO %s (%s) VALUES (%s)" % (tablename, cols, qmarks)
        values = []
        for building in di:
            keylist = []
            for key in building.keys():
                keylist.append(building[key])
            values.append(tuple(keylist))
        return sql, values

#--------------------------test----------------------------------------------------------------------

if __name__ == '__main__':
#     dict = {}
#     url = 'http://ris.szpl.gov.cn/bol/certdetail.aspx?id=34813'
#     response = request.urlopen(url)
#     html_cont = response.read().decode("GBK")
#     soup = BeautifulSoup(html_cont, 'lxml')
#     CertDetail = CertDetail()
#     tablename = 'certdetail'
#     CertDetail.save_certdetail_tomysql(tablename, url, soup)
#     print(list)
    url = 'http://ris.szpl.gov.cn/bol/certdetail.aspx?id=35252'
    id = re.findall("http://ris.szpl.gov.cn/bol/certdetail.aspx\?id=(\d+)", url)
    print(id)
