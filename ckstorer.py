import time
import re
import datetime

import requests
from bs4 import BeautifulSoup
import pymysql


HEADERS = {"User-Agent": 'Mozilla/5.0'}
BASE_URL = 'https://kr.investing.com/news/economy'
BASE_URL_US = 'https://www.investing.com/news/economy'
temp_ = open('./prohibited_words.txt', 'r', encoding='utf-8')
PROHIBITED_WORDS = temp_.readline().replace('\'', '').split(', ')


class CKStorer:
    def __init__(self, database='default_db', table='default_tb'):
        self._user = 'root'
        self._password = 'qw75718856**'
        self.database = database
        self.table = table
        self._connect = None
        self._cur = None
        self._to_do = None
        self.ing = None
        self.overlap_count = 0
        self.overlap_limit = 100

    @property
    def connect(self):
        if self._connect is None:
            self._connect = pymysql.connect(host='localhost', port=3306, user=self.user,
                                            password=self.password, charset='utf8')
        return self._connect

    @property
    def cur(self):
        if self._cur is None:
            self._cur = self.connect.cursor()
        return self._cur

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
            self._cur = self.connect.cursor()
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
                    if self.overlap_count > self.overlap_limit:
                        print(f'기사 중복 {self.overlap_limit + 1}회 발생...')
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
            WHERE char_length(article_content) > 299;
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

            from konlpy.tag import Komoran

            komoran = Komoran(max_heap_size=1024 * 6)
            pos = komoran.pos(content)
            pos = [(re.sub(r'[^가-힣a-zA-Z\s]', '', string[0]), string[1]) for string in pos]
            nnp = [string[0] for string in pos if string[1] == 'NNP' and len(string[0]) > 1
                                                                     and string[0] not in PROHIBITED_WORDS]
            nnp = set(nnp)

            for keyword in nnp:
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

    def relation_keyword(self):
        import logging

        logging.basicConfig(
        format='%(asctime)s : %(levelname)s : %(message)s',
        level=logging.INFO)

        self.connect.select_db('default_db')
        self.cur.execute('''
        SELECT DISTINCT TB.article_content
        FROM default_tb TB, keyword K, keyword_detail KD
        WHERE TB.article_id = KD.article_id
        AND K.keyword_id = KD.keyword_id
        AND char_length(TB.article_content) > 299;
        ''')

        contents = list(self.cur.fetchall())
        from konlpy.tag import Komoran

        komoran = Komoran()
        content_list = []

        for content in contents:
            pos = komoran.nouns(content[0])
            nnp = [string for string in pos if len(string) > 1]
            content_list.append(nnp)

        num_features = 300
        min_word_count = 7
        num_workers = 4
        context = 10
        downsampling = 1e-3

        from gensim.models import word2vec

        model = word2vec.Word2Vec(content_list,
                        workers=num_workers,
                        vector_size=num_features,
                        min_count=min_word_count,
                        window=context,
                        sample=downsampling,
                        sg=0) # sg=0 -> CBOW, sg=1 -> Skip-gram

        model.init_sims(replace=True)
        model_name = 'trained_model'
        model.save(model_name)


if __name__ == '__main__':
    test = CKStorer('test_db', 'test_tb')
    with test as t:
        t.run(1, 10)

    # test.relation_keyword()