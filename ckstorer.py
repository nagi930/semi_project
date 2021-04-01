import time
import re
import datetime

import requests
from bs4 import BeautifulSoup
import pymysql
from konlpy.tag import Komoran

HEADERS = {"User-Agent": 'Mozilla/5.0'}
BASE_URL = 'https://kr.investing.com/news/economy'
temp_ = open('./prohibited_words.txt', 'r', encoding='utf-8')
PROHIBITED_WORDS = temp_.readline().replace('\'', '').split(', ')


class CKStorer:
    def __init__(self, database='default_db', table='default_tb'):
        self._user = 'root'
        self._password = 'qw75718856**'
        self.database = database
        self.table = table
        self._connect = None
        self.cur = None
        self._to_do = None
        self.ing = None
        self.overlap_count = 0

    @property
    def connect(self):
        if self._connect is None:
            self._connect = pymysql.connect(host='localhost', port=3306, user=self.user,
                                            password=self.password, charset='utf8')
        return self._connect

    @property
    def user(self):
        return self._user

    @user.setter
    def user(self, val):
        self._connect = None
        self._user = val

    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, val):
        self._connect = None
        self._password = val

    def __enter__(self):
        if self.cur is None:
            self.cur = self.connect.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connect.commit()
        self.cur.close()
        self.connect.close()
        temp_.close()

    def __str__(self):
        return f'user: {self.user}, database: {self.database}, table: {self.table}'

    def run(self, start, end):
        self.create_crawl_dt()
        self.crawl(start, end)
        self.create_keyword_tb()
        self.extract_keyword()

    def check_overlap(self, column, item):
        self.cur.execute(f'''
        SELECT {column} FROM {self.database}.{self.table}
        WHERE {column} = "{item}"
        ''')
        if self.cur.rowcount == 0:
            return False
        return True

    def create_crawl_dt(self):
        self.cur.execute(f'CREATE DATABASE IF NOT EXISTS {self.database};')
        self.connect.select_db(f'{self.database}')
        self.cur.execute(f'''
        CREATE TABLE IF NOT EXISTS {self.table} (
        article_id INT NOT NULL PRIMARY KEY,
        article_title VARCHAR(100),
        article_publish VARCHAR(20),
        article_date DATETIME,
        article_content LONGTEXT);
        ''')

    def crawl(self, start, end):
        for idx in range(start, end+1):
            url = f'{BASE_URL}/{idx}'
            res = None
            try:
                res = requests.get(url, headers=HEADERS).text.encode('utf-8')
            except requests.exceptions.HTTPError as ex:
                print(ex)
            soup = BeautifulSoup(res, 'lxml')
            temp = soup.find('div', class_='largeTitle')
            articles = temp.find_all('article', class_="js-article-item articleItem ")

            print(f'page: {idx} / {end}')
            for article in articles:
                time.sleep(0.5)
                title = article.find('a', class_='title').text
                title = title.replace('\'', '').replace('\"', '')
                href = article.find('a', class_='title').attrs['href']
                article_num = href.split('/')[-1][8:]
                self.ing = article_num

                if self.check_overlap('article_id', article_num):
                    print(f'{article_num} already in database')
                    self.overlap_count += 1
                    if self.overlap_count > 9:
                        print('기사 중복 10회 발생...')
                        print('--------------extract keyword--------------')
                        return
                    else:
                        continue

                publish = article.find('span', class_='articleDetails').contents[0].string
                content_url = BASE_URL + '/article-' + article_num
                res = None
                try:
                    res = requests.get(content_url, headers=HEADERS).text.encode('utf-8')
                except requests.exceptions.HTTPError as ex:
                    print(ex)
                soup = BeautifulSoup(res, 'lxml')

                date = soup.select_one('div.contentSectionDetails > span').string
                date = re.findall(r'\d+년\s\d+월\s\d+일\s\d+:\d+', date)[0]
                date = date.replace('년 ', '-').replace('월 ', '-').replace('일', '')
                date = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M')

                content = soup.find('div', class_='WYSIWYG articlePage').text
                content = re.sub(r'© Reuters\.s*', '', content)
                content = content.replace(title, '').replace('\'', '').replace('\"', '')\
                                                    .replace('\t', ' ').replace('\n', ' ').strip()
                email_pos = re.search(r'(\S+@\S+)', content)
                if email_pos:
                    content = content[:email_pos.span()[0]]

                self.cur.execute(f'''
                INSERT INTO {self.table} (article_id, article_title, article_publish, article_date, article_content)
                VALUES ({article_num}, "{title}", "{publish}", "{date}", "{content}")
                ''')

                self.connect.commit()

    def create_keyword_tb(self):
        self.cur.execute('''
        CREATE TABLE IF NOT EXISTS keyword_detail(
        num INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        keyword_id INT NOT NULL,
        article_id VARCHAR(50));
        ''')

        self.cur.execute('''
        CREATE TABLE IF NOT EXISTS keyword(
        keyword_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        keyword_name VARCHAR(20) NOT NULL UNIQUE);
        ''')

    @property
    def to_do(self):
        if self._to_do is None:
            self.cur.execute('''
            SELECT DISTINCT article_id FROM default_tb
            ''')
            crawled = self.cur.fetchall()
            crawled = set(x[0] for x in crawled)

            self.cur.execute('''
            SELECT DISTINCT article_id FROM keyword_detail
            ''')
            completed = self.cur.fetchall()
            completed = set(int(x[0]) for x in completed)

            self._to_do = crawled - completed
        return self._to_do

    def extract_keyword(self):
        if not self.to_do:
            print('empty to_do list')
            return
        print(f'total remaining work : {len(self._to_do)}')
        for idx, item in enumerate(self.to_do):
            print(f'start extracting: {item}')
            self.cur.execute(f'''
            SELECT article_content FROM default_tb dt
            WHERE article_id = {item};
            ''')
            content = self.cur.fetchone()
            content = content[0].replace('\t', ' ').replace('\n', ' ').strip()

            komoran = Komoran(max_heap_size=1024 * 6)
            pos = komoran.pos(content)
            nnp = [string[0] for string in pos if string[1] == 'NNP' and len(string[0]) > 1]

            for keyword in nnp:
                keyword = keyword.replace('‘', '').replace('“', '')
                if not keyword or keyword in PROHIBITED_WORDS:
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
                        WHERE article_id = {item}));
                        ''')
                self.connect.commit()
            print(f'complete {item} {idx+1} / {len(self.to_do)}')


if __name__ == '__main__':
    test = CKStorer()
    with test as t:
        t.run(1, 3)