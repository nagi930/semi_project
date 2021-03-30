import time
import re
import datetime
import shutil

import requests
from bs4 import BeautifulSoup
import pymysql
from konlpy.tag import Komoran

class Base:
    def __init__(self, user=None, password=None, database='default_db', table='default_tb'):
        self.user = user or None
        self.password = password or None
        self.database = database
        self.table = table
        self._connect = None
        self.cur = None

    def __enter__(self):
        if self.cur is None:
            self.cur = self.connect.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connect.commit()
        self.cur.close()
        self.connect.close()

    @property
    def connect(self):
        if not self.user or not self.password:
            print('No user, password information')
            return
        if self._connect is None:
            self._connect = pymysql.connect(host='localhost', port=3306, user=self.user,
                                            password=self.password, charset='utf8')
        return self._connect



class InvestingCrawler(Base):
    HEADERS = {"User-Agent":'Mozilla/5.0'}
    BASE = 'https://kr.investing.com/news/economy'
    COMPLETED = None
    ARTICLE_NUMS = None

    def __init__(self, user, password, database, table, start, end):
        super().__init__(user, password, database, table)
        self.start = start
        self.end = end

    def __enter__(self):
        super().__enter__()
        InvestingCrawler.COMPLETED = open('./crawl_completed.txt', 'r+')
        InvestingCrawler.ARTICLE_NUMS = InvestingCrawler.COMPLETED.read().split(',')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        InvestingCrawler.COMPLETED = open('./crawl_completed.txt', 'w+')
        for at in InvestingCrawler.ARTICLE_NUMS:
            InvestingCrawler.COMPLETED.write(f'{at},')
        InvestingCrawler.COMPLETED.close()
        super().__exit__(exc_type, exc_val, exc_tb)

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

    def crawl_(self, start, end):
        for i in range(start, end+1):
            url = f'{InvestingCrawler.BASE}/{i}'
            res = requests.get(url, headers=InvestingCrawler.HEADERS).text.encode('utf-8')

            soup = BeautifulSoup(res, 'lxml')
            temp = soup.find('div', class_='largeTitle')
            articles = temp.find_all('article', class_="js-article-item articleItem ")
            time.sleep(1)
            print(f'now: {i}, total:{end}')
            for article in articles:

                time.sleep(0.5)
                title = article.find('a', class_='title').text
                title = title.replace('\'', '').replace('\"', '')
                href = article.find('a', class_='title').attrs['href']
                article_num = href.split('/')[-1][8:]
                print(article_num)
                if article_num in InvestingCrawler.ARTICLE_NUMS:
                    print(f'Already exists article')
                    continue
                else:
                    InvestingCrawler.ARTICLE_NUMS.append(article_num)
                publish = article.find('span', class_='articleDetails').contents[0].string

                content_url = InvestingCrawler.BASE + '/article-' + article_num
                res = requests.get(content_url, headers=InvestingCrawler.HEADERS).text.encode('utf-8')
                soup = BeautifulSoup(res, 'lxml')

                date = soup.select_one('div.contentSectionDetails > span').string
                date = re.findall(r'\d+년\s\d+월\s\d+일\s\d+\:\d+', date)[0]
                date = date.replace('년 ', '-').replace('월 ', '-').replace('일', '')
                date = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M')

                content = soup.find('div', class_='WYSIWYG articlePage').text
                content = re.sub(r'© Reuters\.s*', '', content)
                content = content.replace(title, '').replace('\'', '').replace('\"', '').replace('\t', ' ').replace('\n', ' ').strip()
                email_pos = re.search('(\S+@\S+)', content)
                if email_pos:
                    content = content[:email_pos.span()[0]]

                self.cur.execute(f'''
                INSERT INTO {self.table} (article_id, article_title, article_publish, article_date, article_content)
                VALUES ("{article_num}", "{title}", "{publish}", "{date}", "{content}")
                ''')
            self.connect.commit()

    def save_data(self):
        self.create_dt()
        self.crawl_(self.start, self.end)


class Keyword(Base):
    def __init__(self, user, password, database, table):
        super().__init__(user, password, database, table)

    def __enter__(self):
        super().__enter__()
        shutil.copy('./crawl_completed.txt', './crawl_completed_copy.txt')
        self.f = open('./crawl_completed_copy.txt', 'r')
        self.to_work = self.f.read().split(',')
        self.keyword_completed = open('./keyword_completed.txt', 'r+')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.f = open('./crawl_completed_copy.txt', 'w')
        if self.to_work:
            self.f.write(','.join(self.to_work))
        self.f.close()
        self.keyword_completed.close()
        super().__exit__(exc_type, exc_val, exc_tb)

    def keyword_(self):
        while self.to_work:
            self.now_working = None
            while not self.now_working and self.to_work:
                self.now_working = self.to_work.pop().strip()
            print(self.now_working)
            self.connect.select_db('default_db')
            self.cur.execute(f'''
            SELECT article_id, article_content FROM default_tb dt
            WHERE article_id = {self.now_working};
            ''')
            id, content = self.cur.fetchone()
            # nouns = set(Komoran().nouns(content))
            content = content.replace('\t', ' ').replace('\n', ' ').strip()
            komoran = Komoran(max_heap_size=1024 * 6)
            pos = komoran.pos(content)
            NNP = [string[0] for string in pos if string[1] == 'NNP' and len(string[0]) > 1]
            self.insert(NNP)

    def insert(self, keywords):
        self.cur.execute('''
        CREATE TABLE IF NOT EXISTS keyword_detail(
        num INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        keyword_id VARCHAR(20) NOT NULL,
        article_id VARCHAR(50));
        ''')

        self.cur.execute('''
        CREATE TABLE IF NOT EXISTS keyword(
        keyword_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        keyword_name VARCHAR(20) NOT NULL UNIQUE)
        ''')

        for keyword in keywords:
            keyword = keyword.replace('‘', '').replace('“', '')
            if not keyword or keyword == '한경' or keyword == '사진':
                continue
            self.cur.execute(f'''
            INSERT IGNORE INTO keyword
            VALUES (NULL, "{keyword}")
            ''')

            self.connect.commit()

            self.cur.execute(f'''
            INSERT INTO keyword_detail (keyword_id, article_id)
            VALUES ((SELECT keyword_id
                    FROM keyword
                    WHERE keyword_name = "{keyword}"),
                    (SELECT article_id
                    FROM default_tb
                    WHERE article_id = {self.now_working}));
                    ''')
            self.connect.commit()
        self.keyword_completed.write(f'{self.now_working},')


if __name__ == '__main__':
    crawl = InvestingCrawler('root', 'qw75718856**', 'default_db', 'default_tb', 23, 70)
    kw = Keyword('root', 'qw75718856**', 'default_db', 'default_tb')
    # with crawl as c:
    #     c.save_data()
    with kw as k:
        k.keyword_()
