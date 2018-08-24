from realty_sprider.dbConnect import DBUtils
import time
class UrlManager(object):
    def __init__(self):
        self.new_urls=set()
        self.old_urls=set()
        self.url=''
    def add_new_url(self, url):
        if url is None:
            return
        if url not in self.new_urls and url not in self.old_urls:
            self.new_urls.add(url)

    def add_new_urls(self, urls):
        if urls is None or len(urls)==0:
            return
        for url in urls:
            self.add_new_url(url)

    def has_new_url(self):
        return len(self.new_urls)!=0

    def get_new_url(self):
        new_url=self.new_urls.pop()
        self.url = new_url
        self.old_urls.add(new_url)
        return new_url

    def get_old_urls(self):
        return self.old_urls

    def get_urls_set(self):
        return self.new_urls

    def get_current_url(self):
         return self.url

    def save_url(self):
        url=self.get_current_url()
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        insert_sql='INSERT INTO craw_url (urls,timestamp) VALUES (%s,%s)'
        list=[url,timestamp]
        DBUtils.execute_insert(insert_sql,list)
