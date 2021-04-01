import time
import re
import datetime
import shutil

import requests
from bs4 import BeautifulSoup
import pymysql
from konlpy.tag import Komoran

class Base:
    def __init__(self, user=None, password=None, database=None, table=None):
        self.user = user or None
        self.password = password or None
        self.database = database or None
        self.table = table or None
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
    READY_TO_KEYWORD =None
    CONTINUE_CNT = 0

    def __init__(self, user, password, database, table, start, end):
        super().__init__(user, password, database, table)
        self.start = start
        self.end = end

    def __enter__(self):
        super().__enter__()
        InvestingCrawler.COMPLETED = open('./crawl_completed.txt', 'r+')
        InvestingCrawler.ARTICLE_NUMS = InvestingCrawler.COMPLETED.read().split(',')
        InvestingCrawler.COMPLETED = open('./crawl_completed.txt', 'w+')

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        InvestingCrawler.COMPLETED.close()
        shutil.copy('./crawl_completed.txt', './ready_to_keyword.txt')
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
            res = None
            try:
                res = requests.get(url, headers=InvestingCrawler.HEADERS).text.encode('utf-8')
            except requests.exceptions.HTTPError:
                print("can't connect to url")

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
                    InvestingCrawler.CONTINUE_CNT += 1
                    if InvestingCrawler.CONTINUE_CNT > 2:
                        print('3 회 중복')
                        return
                    continue
                else:
                    InvestingCrawler.COMPLETED.write(f'{article_num},')
                publish = article.find('span', class_='articleDetails').contents[0].string

                content_url = InvestingCrawler.BASE + '/article-' + article_num
                res = None
                try:
                    res = requests.get(content_url, headers=InvestingCrawler.HEADERS).text.encode('utf-8')
                except requests.exceptions.HTTPError:
                    print("can't connect to url")
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
    PROHIBITED_WORD = ['한경', '사진', '19', '만원', '있다', '미국', '중국', '일본',
                       '이달', '분기', '오전', '오후', '1월', '한국', '인도네시아',
                       '2월', '3월', '4월', '5월', '6월',
                       '7월', '8월', '9월', '10월', '11월', '12월', '기준', '인수', '관리', '해지',
                       '코리아', '투자', '코스닥', '시장', '거래', '서울', '부총리', '이용한', '보건',
                       '한국', '주가', '지수', '경기', '가격', '공시', '계약', '매도', '매수',
                       '사업', '부회장', '이익', '은행', '연합뉴스', '회사', '디지털', '환율',
                       '그룹', '위원회', '영업', '부동산', '이사', '상장', '주주', '주주총회',
                       '위원', '모델', '자산', '연구원', '지주', '금리', '마케팅', '자산운용',
                       '산업', '혁신', '법인', '수익률', '세대', '이사회', '평가', '담보', '부품',
                       '대형', '본사', '보험', '정기', '글로벌', '가구', '환경', '서비스', '공인',
                       '대표', '조기', '프로젝트', '구독', '포인트', '로이터', '통신', '브랜드',
                       '모바일', '구독', '센터', '스마트', '패치', '페이', '케이', '국채', '뉴욕',
                       '정비', '자본', '기차', '수수료', '디자인', '맥주', '와인', '이해충돌', '주식',
                       '대부', '플랫폼', '위안', '가치주', '리서치', '친환경', '금융', '시가총액', '순위',
                       '수소', '섬유', '엔진', '주자', '퓨얼', '연료전지', '대기업']
    def __init__(self, user, password, database, table):
        super().__init__(user, password, database, table)

    def __enter__(self):
        super().__enter__()
        self.f = open('ready_to_keyword.txt', 'r')
        self.to_work = self.f.read().split(',')
        self.keyword_completed = open('./keyword_completed.txt', 'r+')

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.f = open('ready_to_keyword.txt', 'w')
        self.f.close()
        self.keyword_completed.close()
        shutil.copy('./keyword_completed.txt', './crawl_completed.txt')
        super().__exit__(exc_type, exc_val, exc_tb)

    def keyword_(self):
        while self.to_work:
            self.now_working = self.to_work.pop().strip()
            print(self.now_working)
            self.connect.select_db('default_db')
            try:
                self.cur.execute(f'''
                SELECT article_id, article_content FROM default_tb dt
                WHERE article_id = {self.now_working};
                ''')
            except pymysql.err.ProgrammingError:
                print('작업할 아이템이 없습니다.')
            if not self.cur.rowcount:
                continue
            _, content = self.cur.fetchone()
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
            if not keyword or keyword in Keyword.PROHIBITED_WORD:
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
    info = ['root', 'qw75718856**', 'default_db', 'default_tb']
    crawl = InvestingCrawler(*info, 1, 100)
    kw = Keyword(*info)

    with crawl as c:
        c.save_data()
    with kw as k:
        k.keyword_()
