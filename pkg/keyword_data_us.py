import pymysql
import matplotlib.font_manager as fm
from matplotlib import rc


class DatabaseConnect:
    def __enter__(self):
        self.conn = pymysql.connect(host='localhost', port=3306, user='root',
                               password='qw75718856**', charset='utf8')
        self.cur = self.conn.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cur.close()
        self.conn.close()

def get_week_list():
    database = DatabaseConnect()
    with database:
        database.cur.execute(f'''
        SELECT K.keyword_name day_keywords, COUNT(K.keyword_id) 횟수
        FROM default_db2.keyword_detail2 KD, default_db2.keyword2 K, default_db2.default_tb2 DT
        WHERE KD.keyword_id = K.keyword_id
        AND KD.article_id = DT.article_id
        AND DATE(DT.article_date) BETWEEN DATE_ADD(NOW(),INTERVAL -1 WEEK ) AND NOW()
        GROUP BY KD.keyword_id
        ORDER BY 횟수 DESC
        LIMIT 20;
        ''')

        if not database.cur.rowcount:
            week_keywords = 0
        else:
            week_keywords = database.cur.fetchall()
    day_list, day_count = zip(*week_keywords)
    return day_list, day_count

def get_month_keyword():
    database = DatabaseConnect()
    with database:
        database.cur.execute('''
        SELECT K.keyword_name 키워드, COUNT(K.keyword_id) 횟수
        FROM default_db2.keyword_detail2 KD, default_db2.keyword2 K, default_db2.default_tb2 DT
        WHERE KD.keyword_id = K.keyword_id
        AND KD.article_id = DT.article_id
        AND DATE(DT.article_date) BETWEEN DATE_ADD(NOW(),INTERVAL -1 MONTH ) AND NOW()
        GROUP BY KD.keyword_id
        ORDER BY 횟수 DESC
        LIMIT 100;
        ''')

        month_keyword = database.cur.fetchall()
    return month_keyword

def get_day_by_day_count(keyword, dates):
    database = DatabaseConnect()
    count = []
    with database:
        for date in dates:
            database.cur.execute(f'''
            SELECT COUNT(K.keyword_id) 횟수
            FROM default_db2.keyword_detail2 KD, default_db2.keyword2 K, default_db2.default_tb2 DT
            WHERE KD.keyword_id = K.keyword_id
            AND KD.article_id = DT.article_id
            AND K.keyword_name = "{keyword}"
            AND DATE(DT.article_date) = "{date}"
            GROUP BY KD.keyword_id;
            ''')
            if not database.cur.rowcount:
                f = 0
            else:
                f = database.cur.fetchone()[0]
            count.append(f)
    return count

def get_week_keyword():
    database = DatabaseConnect()
    with database:
        database.cur.execute('''
        SELECT K.keyword_name 키워드, COUNT(K.keyword_id) 횟수
        FROM default_db2.keyword_detail2 KD, default_db2.keyword2 K, default_db2.default_tb2 DT
        WHERE KD.keyword_id = K.keyword_id
        AND KD.article_id = DT.article_id
        AND DATE(DT.article_date) BETWEEN DATE_ADD(NOW(),INTERVAL -1 WEEK ) AND NOW()
        GROUP BY KD.keyword_id
        ORDER BY 횟수 DESC
        LIMIT 100;
        ''')

        week_keyword = database.cur.fetchall()
    return week_keyword

def keyword_network(keyword_list):
    from gensim.models import Word2Vec

    font_name = fm.FontProperties(fname='./static/fonts/NanumBarunpenB.ttf').get_name()
    rc('font', family=font_name)

    kws = keyword_list
    integration_node = []
    integration_edge = []
    loaded_model = Word2Vec.load('./trained_model2')
    print(loaded_model.wv.index_to_key)

    for i, kw in enumerate(kws):
        relation_words = loaded_model.wv.most_similar(kw)
        relation_words = dict(relation_words)
        node_dataset = [{'id': idx, 'label': relation_word} for idx, relation_word in enumerate(relation_words, i*11+1)]
        node_dataset.append({'id': i*11, 'label': kw})
        integration_node.extend(node_dataset)

        edge_dataset = [{'from': i*11, 'to': idx, 'width': 1} for idx in range(11*i+1, len(node_dataset)+i*11)]
        integration_edge.extend(edge_dataset)

    return integration_node, integration_edge


if __name__ =='__main__':
    print(keyword_network(['covid-19']))