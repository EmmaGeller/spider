import logging
from bs4 import BeautifulSoup
from urllib import request
from realty_sprider.dbConnect import DBUtils
import re
import time
#----------------------------------------------------
# presale_items表
#----------------------------------------------------
class PresellProject(object):

    def _get_presellproject_contents(self,url, soup):
        tables = soup.find_all('table', id='DataList1')
        table = tables[0].find_all('table')
        projectlist = []
        for tr in table[0].find_all('tr'):
            tdlist = []
            for td in tr.find_all('td'):
                tdlist.append(td.text.strip())
            for a in tr.find_all('a'):
                if (len(a['href']) != 0):
                    resu = a['href'] + '/'
                    ids = re.findall(r'\./certdetail.aspx\?id=(.*?)/', resu)
                    if (len(ids) != 0):
                        id = ids[0]
                        tdlist.append(id)
            projectlist.append(tdlist)
        return projectlist

    def _presellproject_data(self,url, soup):
        projectlist = self._get_presellproject_contents(url,soup)
        cleanprojects = []
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        for project in projectlist:
            if (len(project) >= 2):
                project.append(timestamp)
                project.append(url)
                cleanprojects.append(tuple(project))
        return cleanprojects

    # 保存数据到mysql
    def save_presellproject_tomysql(self, url,soup):
        projects = self._presellproject_data(url,soup)
        qmarks = ','.join(['%s'] * len(projects[0]))
        sql = """INSERT INTO  presale_items(serial_num,presell_certificate,project_name,develop_enterprise,
                area,approval_time,id,timestamp,url)  VALUES (%s) 
              """ % (qmarks)
        return sql,projects
        # try:
        #     DBUtils.execute_insertmany(sql,projects)
        #     return 'success'
        # except Exception as e:
        #     logging.error('插入数据库出错！！交给上层处理！！！')
        #     raise e


# if __name__ == '__main__':
#     dict = {}
#     url = 'http://ris.szpl.gov.cn/bol/'
#     response = request.urlopen(url)
#     html_cont = response.read().decode("GBK")
#     soup = BeautifulSoup(html_cont, 'lxml')
#     presellProject = PresellProject()
#     presellProject.save_presellproject_tomysql(url, soup)
