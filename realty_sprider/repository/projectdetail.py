import time
import pymysql
from bs4 import BeautifulSoup
from urllib import request
from urllib import parse
import re
import logging
from realty_sprider.dbConnect import DBUtils
#----------------------------------------------------
# price_regulatory表------------projectdetail表
#----------------------------------------------------
class ProjectDetail(object):
    # 获取项目详细资料表内容：list
    def _get_projectdetail_contents(self,url,soup):
        projectdetail = []
        trs = soup.find_all('tr', class_='a1')
        for tr in trs:
            tdlist = []
            for td in tr.find_all('td'):
                tdlist.append(td.text.strip())
            projectdetail.append(tdlist)
        project_buildings= self._project_building_info(url,soup)
        return projectdetail, project_buildings

    # <table id="DataList1"
    def _project_building_info(self, url,soup):
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        tables = soup.find_all('table', id='DataList1')
        if len(tables)!=0:
                trlist = []
                for tr in tables[0].find_all('tr', attrs={'bgcolor': '#F5F9FC'}):
                    tdlist = []
                    for td in tr.find_all('td'):
                        if (td.a != None):
                            try:
                                resu=td.a['href']+'/'
                                ids = re.findall(r'building.aspx\?id=(.*?)&presellid=(.*?)/',resu)
                                building_id=ids[0][0]
                                presellid=ids[0][1]
                                tdlist.append(building_id)
                                tdlist.append(presellid)
                                tdlist.append(url)
                                tdlist.append(timestamp)
                            except Exception as e:
                                logging.error(url)
                                logging.error('ProjectDetail正则表达式出错！！，当前id=%s, erro=%s'%(building_id,e))
                        else:
                            tdlist.append(td.text.strip())
                    trlist.append(tdlist)
                return trlist
        else:
            return None

    # 判断是否是所需字段：返回数据库对应字段!
    def _is_need(self, content):
        keydict = {'项目名称': 'project_name', '宗地号': 'zong_num',
                   '宗地位置': 'zong_location', '受让日期': 'let_date', '所在区域': 'area', '权属来源': 'ownership_source',
                   '批准机关': 'approval_authority', '合同文号': 'contract_num', '使用权限': 'use_permission',
                   '补充协议': 'supplemental', '用地规划许可证': 'land_use_permit', '房屋用途': 'house_using',
                   '土地用途': 'house_using', '土地等级': 'land_grade', '基地面积': 'basic_area', '宗地面积': 'zong_area',
                   '总建筑面积': 'total_area', '预售总套数': 'total_presell_set', '预售总面积': 'total_presell_area',
                   '现售总面积': 'total_sell_cash_area', '现售总套数': 'total_sell_cash_set', '售楼电话一': 'sales_tel_one',
                   '售楼电话二': 'sales_tel_two', '工程监管机构': 'engineering_regulatory',
                   '物业管理公司': 'property_management_company',
                   '管理费': 'management_fee', '备注': 'note'}
        value = None
        if (content in keydict.keys()):
            value = keydict[content]
        return value

    # 判断是否是价款监管机构表
    def price_regulatory(self, content):
        keydict = {'价款监管机构': 'price_regulatory', '账户名称': 'account_name', '账号': 'account_num'}
        value = None
        pattern = re.compile('价款监管机构.')
        if (content in keydict.keys()):
            value = keydict[content]
        elif (re.match(pattern, content) != None):
            value = keydict['价款监管机构']
        return value


    # 保存项目详细资料表到dict
    def _projectdetail_contents(self,url,soup):
        projectdetaillist, project_buildings = self._get_projectdetail_contents(url,soup)
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        dict = {}
        price_reg_list = []
        result = parse.urlparse(url)
        di = parse.parse_qs(result.query)
        presell_id = di['id']
        dict['id'] = presell_id
        dict['url'] = url
        dict['timestamp'] = timestamp
        price_reg_dict = {}
        for i in range(len(projectdetaillist)):
            j = 0;
            while j < len(projectdetaillist[i]):
                k = projectdetaillist[i][j]
                key = self._is_need(projectdetaillist[i][j])
                if (key != None):
                    value = projectdetaillist[i][j + 1]
                    dict[key] = value
                elif (self.price_regulatory(k) != None):
                    price_reg_key = self.price_regulatory(k)
                    value = projectdetaillist[i][j + 1]
                    price_reg_dict[price_reg_key] = value;
                    if (self.price_regulatory(k) == 'account_num' and len(price_reg_dict) != 0):
                        price_reg_dict['presell_id'] = presell_id
                        price_reg_dict['url'] = url
                        price_reg_dict['timestamp'] = timestamp
                        price_reg_list.append(price_reg_dict)
                        price_reg_dict = {}
                j = j + 2
        return dict,price_reg_list,project_buildings,presell_id

        # 保存数据到mysql
    def save_projectdetail_tomysql(self, tablename, url, soup):
        projectdetaildict, price_reg_list, project_buildings,presell_id = self._projectdetail_contents(url, soup)
        qmarks = ','.join(['%s'] * len(projectdetaildict))  # 用于替换记录值
        cols = ','.join(projectdetaildict.keys())  # 字段名
        projectdetails = []
        sql = "INSERT INTO %s (%s) VALUES (%s)" % (tablename, cols, qmarks)
        for key in projectdetaildict.keys():
            projectdetails.append(projectdetaildict[key])
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        if(project_buildings==None):
            infosql="""INSERT INTO project_building_info (presell_id,project_name,url,timestamp) VALUES (%s,%s,%s,%s)"""
            project_buildings=[(presell_id,'该项目尚未录入价款监管机构',url,timestamp)]
        else:
            infosql = self._save_project_buding_info()
        if(len(price_reg_list)==0):
            regs_sql="""INSERT INTO  price_regulatory (price_regulatory,presell_id, url,timestamp) VALUES (%s,%s,%s,%s)"""
            reg_values=[('该项目尚未录入价款监管机构',presell_id,url,timestamp)]
        else:
            regs_sql, reg_values = self._save_price_reglist(price_reg_list)
        return sql, projectdetails,regs_sql,reg_values,infosql, project_buildings
        #
        # try:
        #     conn = DBUtils.getConnect()
        #     cur = conn.cursor()
        #     cur.execute(sql, projectdetails)
        #     cur.executemany(regs_sql,reg_values)
        #     cur.executemany(infosql, project_buildings)
        #     data = cur.fetchall()
        #     conn.commit()
        #     return 'success'
        # except pymysql.Error as e:
        #     print ("Error1 %d: %s" % (e.args[0], e.args[1]))
        #     time.sleep(60)
        #     conn.rollback()
        #     logging.error('插入数据库出错！！，交给上层处理！tablename=' + tablename)
        #     raise e
        # finally:
        #     cur.close()
        #     conn.close()

    def _save_price_reglist(self, reglist):
        qmarks = ','.join(['%s'] * len(reglist[0]))  # 用于替换记录值
        cols = ','.join(reglist[0].keys())  # 字段名
        tablename='price_regulatory'
        sql = "INSERT INTO %s (%s) VALUES (%s)" % (tablename, cols, qmarks)
        values=[]
        for reg in reglist:
            keylist = []
            for key in reg.keys():
                keylist.append(reg[key])
            values.append(tuple(keylist))
        return sql,values

    def _save_project_buding_info(self):
        sql = """
              INSERT INTO project_building_info (project_name,
              building_name,building_permit,construction_permit,building_id,
              presell_id,url,timestamp) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
              """
        return sql;

if __name__ == '__main__':
    url = 'http://ris.szpl.gov.cn/bol/projectdetail.aspx?id=1014'
    response = request.urlopen(url)
    html_cont = response.read().decode("GBK")
    soup = BeautifulSoup(html_cont, 'lxml')
    ProjectDetail=ProjectDetail()
    tablename = 'projectdetail'
    ProjectDetail.save_projectdetail_tomysql(tablename,url,soup)
    # d, i, m,n = ProjectDetail.projectdetail_contents(url,soup)
    # print(d)
    # print(i)
    # print(m)
    # print(n)



