from collections import Counter

import pandas as pd
from konlpy.tag import Hannanum
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
from sqlalchemy import create_engine, types

# class Keyword:
#     def __enter__(self):
#         self.engine = create_engine(
#             f'mysql+mysqldb://root:qw75718856**@localhost/default_db', encoding='utf-8')
#         self.conn = pymysql.connect(host='localhost', port=3306, user='root',
#                                     password='qw75718856**', charset='utf8')
#         self.cur = self.conn.cursor()
#         return self
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         self.engine.dispose()
#         self.conn.commit()
#         self.cur.close()
#         self.conn.close()
#
#
#
# k = Keyword()
# with k as kk:
#     df = pd.read_sql('SELECT article_content FROM default_db.default_tb', con=kk.conn)
#     for idx in range(0, 1):
#         content = df.iloc[idx, 0]
#         pos = Hannanum().pos(content)
#         cnt = Counter(pos)
#         keywords = [(x[0], cnt[x]) for x in cnt if (x[1] == 'N')
#                                                     and (len(x[0]) > 1)
#                                                     and (cnt[x] > 1)]
#
#         keyword_df = pd.DataFrame(keywords, columns=['keyword_name', 'keyword_cnt'])
#         print(keyword_df)
#












class Keyword:
    def __enter__(self):
        self.f = open('./crawl_completed.txt', 'r+')
        self.to_work = self.f.read().split(',')
        self.keyword_completed = open('./keyword_completed.txt', 'r+')
        self.conn = pymysql.connect(host='localhost', port=3306, user='root',
                                    password='qw75718856**', charset='utf8')
        self.cur = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.f = open('./crawl_completed.txt', 'w')
        if self.to_work:
            self.f.write(','.join(self.to_work))
        self.f.close()
        self.cur.close()
        self.conn.close()


    def keyword_(self):
        while self.to_work:
            self.now_working = None
            while not self.now_working:
                self.now_working = self.to_work.pop().strip()
            self.conn.select_db('default_db')
            self.cur.execute(f'''
            SELECT article_id, article_content FROM default_tb dt
            WHERE article_id = {self.now_working};
            ''')
            id, content = self.cur.fetchone()
            pos = Hannanum().pos(content)
            cnt = Counter(pos)
            keywords = [(x[0], cnt[x]) for x in cnt if (x[1] == 'N') and (len(x[0]) > 1) and (cnt[x] > 1)]
            self.insert(keywords)

    def insert(self, keywords: list):
        self.cur.execute('''
        CREATE TABLE IF NOT EXISTS keyword_detail(
        num INT NOT NULL AUTO_INCREMENT,
        keyword_name VARCHAR(20) NOT NULL,
        article_id VARCHAR(50)
        PRIMARY KEY (num, keyword_name))
        ENGINE = MyISAM;
        ''')

        self.cur.execute('''
        CREATE TABLE IF NOT EXISTS keyword(
        keyword_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        keyword_name VARCHAR(20) NOT NULL)
        ''')
        for keyword, cnt in keywords:
            print(keyword)
            self.cur.execute(f'''
            INSERT IGNORE INTO keyword (keyword_name)
            VALUES ("{keyword}")
            ''')

            self.conn.commit()

            self.cur.execute(f'''
            INSERT INTO keyword_detail (keyword_name, article_title)
            VALUES (SELECT keyword_id
                    FROM keyword
                    WHERE keyword_name = "{keyword}",
                    SELECT article_name
                    FROM default_tb
                    WHERE id = {self.now_working});
                    ''')
            self.conn.commit()
        self.keyword_completed.write(f'{self.now_working},')

if __name__ == '__main__':
    test = Keyword()
    with test as t:
        t.keyword_()
