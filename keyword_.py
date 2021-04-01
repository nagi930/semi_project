import shutil

from konlpy.tag import Mecab
import pymysql


class Keyword:
    def __enter__(self):
        shutil.copy('./crawl_completed.txt', 'ready_to_keyword.txt')
        self.f = open('ready_to_keyword.txt', 'r+')
        self.to_work = self.f.read().split(',')
        self.keyword_completed = open('./keyword_completed.txt', 'r+')
        self.conn = pymysql.connect(host='localhost', port=3306, user='root',
                                    password='qw75718856**', charset='utf8')
        self.cur = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.f = open('ready_to_keyword.txt', 'w')
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
            print(self.now_working)
            self.conn.select_db('default_db')
            self.cur.execute(f'''
            SELECT article_id, article_content FROM default_tb dt
            WHERE article_id = {self.now_working};
            ''')
            if not self.cur.rowcount:
                continue
            id, content = self.cur.fetchone()
            nouns = set(Mecab().nouns(content))
            self.insert(nouns)

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

        # self.cur.execute('''
        # ALTER TABLE keyword DROP PRIMARY KEY, ADD PRIMARY KEY (keyword_id, keyword_name)
        # ''')
        #
        # self.cur.execute('''
        # ALTER TABLE keyword ADD UNIQUE INDEX (keyword_name)
        # ''')

        for keyword in keywords:
            keyword = keyword.replace('‘', '').replace('“', '')
            if not keyword:
                continue
            self.cur.execute(f'''
            INSERT IGNORE INTO keyword
            VALUES (NULL, "{keyword}")
            ''')

            self.conn.commit()

            self.cur.execute(f'''
            INSERT INTO keyword_detail (keyword_id, article_id)
            VALUES ((SELECT keyword_id
                    FROM keyword
                    WHERE keyword_name = "{keyword}"),
                    (SELECT article_id
                    FROM default_tb
                    WHERE article_id = {self.now_working}));
                    ''')
            self.conn.commit()
        self.keyword_completed.write(f'{self.now_working},')

if __name__ == '__main__':
    test = Keyword()
    with test as t:
        t.keyword_()
# 608902 610169