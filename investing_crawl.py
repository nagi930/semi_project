import time
import re
import datetime

import requests
from bs4 import BeautifulSoup
import pymysql


class InvestingCrawler:
    HEADERS = {"User-Agent":'Mozilla/5.0'}
    BASE = 'https://kr.investing.com/news/economy'
    COMPLETED = None
    art_nums = None

    def __init__(self, user='', password='', database='default_db', table='default_tb'):
        self.user = user
        self.password = password
        self.database = database
        self.table = table
        self._connect = None
        self.cur = None

    @property
    def connect(self):
        if not self.user or not self.password:
            print('No user, password information')
            return
        if self._connect is None:
            self._connect = pymysql.connect(host='localhost', port=3306, user=self.user,
                                            password=self.password, charset='utf8')
        return self._connect

    def __enter__(self):
        self.cur = self.connect.cursor()
        InvestingCrawler.COMPLETED = open('./crawl_completed.txt', 'r+')
        InvestingCrawler.art_nums = InvestingCrawler.COMPLETED.read().split(',')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connect.commit()
        self.cur.close()
        self.connect.close()
        InvestingCrawler.COMPLETED.close()
        InvestingCrawler.COMPLETED = open('./crawl_completed.txt', 'w+')
        for at in InvestingCrawler.art_nums:
            InvestingCrawler.COMPLETED.write(f'{at},')
        InvestingCrawler.COMPLETED.close()

    def create_dt(self):
        self.cur.execute(f'CREATE DATABASE IF NOT EXISTS {self.database}')
        self.connect.select_db(f'{self.database}')
        self.cur.execute(f'''
        CREATE TABLE IF NOT EXISTS {self.table} (
        article_id INT NOT NULL PRIMARY KEY,
        article_title VARCHAR(100),
        article_publish VARCHAR(20),
        article_date DATETIME,
        article_content LONGTEXT);
        ''')

    def start(self, start=60, end=65):
        self.create_dt()
        self.crawl_(start, end)

    def crawl_(self, start=60, end=65):
        for i in range(start, end):
            url = f'{InvestingCrawler.BASE}/{i}'
            res = requests.get(url, headers=InvestingCrawler.HEADERS).text.encode('utf-8')

            soup = BeautifulSoup(res, 'lxml')
            temp = soup.find('div', class_='largeTitle')
            articles = temp.find_all('article', class_="js-article-item articleItem ")
            time.sleep(1)
            print(f'now: {i}, total:{end-1}')
            for article in articles:
                time.sleep(0.5)
                title = article.find('a', class_='title').text
                title = title.replace('\'', '').replace('\"', '')
                href = article.find('a', class_='title').attrs['href']
                article_num = href.split('/')[-1][8:]

                if article_num in InvestingCrawler.art_nums:
                    print(f'Already exists article')
                    continue
                else:
                    InvestingCrawler.art_nums.append(article_num)
                publish = article.find('span', class_='articleDetails').contents[0].string

                content_url = InvestingCrawler.BASE + '/article-' + article_num
                res = requests.get(content_url, headers=InvestingCrawler.HEADERS).text.encode('utf-8')
                soup = BeautifulSoup(res, 'lxml')

                date = soup.select_one('div.contentSectionDetails > span').string
                date = re.match(r'\d+년\s\d+월\s\d+일\s\d+\:\d+', date).group()
                date = date.replace('년 ', '-').replace('월 ', '-').replace('일', '')
                date = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M')

                content = soup.find('div', class_='WYSIWYG articlePage').text
                content = re.sub(r'© Reuters\.s*', '', content)
                content = content.replace(title, '').replace('\'', '').replace('\"', '').strip()
                email_pos = re.search('(\S+@\S+)', content)
                if email_pos:
                    content = content[:email_pos.span()[0]]

                self.cur.execute(f'''
                INSERT INTO {self.table} (article_id, article_title, article_publish, article_date, article_content)
                VALUES ("{article_num}", "{title}", "{publish}", "{date}", "{content}")
                ''')
            self.connect.commit()



if __name__ == '__main__':
    invest = InvestingCrawler('root', 'qw75718856**')
    with invest as iv:
        iv.start()





# HEADERS = {"User-Agent":'Mozilla/5.0'}
# base = 'https://kr.investing.com/news/economy'
# conn = pymysql.connect(host='localhost', port=3306, user='root',
#                         password='qw75718856**', charset='utf8')
# cur = conn.cursor()
#
# # cur.execute('CREATE DATABASE investing')
# conn.select_db('investing')
# cur.execute('''
# CREATE TABLE IF NOT EXISTS article (
# id INT NOT NULL PRIMARY KEY,
# title VARCHAR(50),
# publish VARCHAR(20),
# date DATETIME,
# content LONGTEXT)
# ''')
# article_list = []
#
# for i in range(4, 5):
#     url = f'{base}/{i}'
#     res = requests.get(url, headers=HEADERS).text.encode('utf-8')
#
#     soup = BeautifulSoup(res, 'lxml')
#     temp = soup.find('div', class_='largeTitle')
#     articles = temp.find_all('article', class_="js-article-item articleItem ")
#     time.sleep(1)
#
#     for article in articles:
#         time.sleep(0.5)
#         title = article.find('a', class_='title').text
#         title = title.replace('\'', '').replace('\"', '')
#         href = article.find('a', class_='title').attrs['href']
#         article_num = href.split('/')[-1][8:]
#         if article_num in article_list:
#             raise ValueError('Already exists article')
#         else:
#             article_list.append(article_num)
#         publish = article.find('span', class_='articleDetails').contents[0].string
#
#
#
#
#         content_url = base + '/article-' + article_num
#         res = requests.get(content_url, headers=HEADERS).text.encode('utf-8')
#         soup = BeautifulSoup(res, 'lxml')
#
#         date = soup.select_one('div.contentSectionDetails > span').string
#         date = re.search(r'\(.*\)', date).group().strip('()')
#         date = date.replace('년 ', '-').replace('월 ', '-').replace('일', '')
#         date = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M')
#
#         content = soup.find('div', class_='WYSIWYG articlePage').text
#         content = re.sub(r'© Reuters\.s*', '', content)
#         content = content.replace(title, '').replace('\'', '').replace('\"', '').strip()
#         m = re.search('(\S+@\S+)', content)
#         if m:
#             content = content[:m.span()[0]]
#         print(article_num, title, publish, date, content)
#         pos = Hannanum().pos(content)
#         cnt = Counter(pos)
#         g = (e for e in cnt if e[1] == 'N')
#         print(list(g))
#
#
#         # cur.execute(f'''
#         # INSERT INTO article (id, title, publish, date, content)
#         # VALUES ("{article_num}", "{title}", "{publish}", "{date}", "{content}")
#         # ''')
# conn.commit()
# cur.close()
# conn.close()
