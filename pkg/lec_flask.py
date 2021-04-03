import json
import datetime

from flask import Flask, render_template, request
import pymysql
import pandas_datareader.data as web
from wordcloud import WordCloud
from gensim.models import Word2Vec
import matplotlib.font_manager as fm
from matplotlib import rc

app = Flask(__name__, template_folder='templates')

now = datetime.datetime.now()
today = now.strftime('%Y-%m-%d')
yesterday = (now - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
week_ago = now - datetime.timedelta(weeks=1)
week = [(now - datetime.timedelta(days=x)).strftime('%Y-%m-%d') for x in reversed(range(1, 8))]

conn = pymysql.connect(host='localhost', port=3306, user='root',
                       password='qw75718856**', charset='utf8')
cur = conn.cursor()
cur.execute('''
SELECT K.keyword_name 키워드, COUNT(K.keyword_id) 횟수
FROM default_db.keyword_detail KD, default_db.keyword K, default_db.default_tb DT
WHERE KD.keyword_id = K.keyword_id
AND KD.article_id = DT.article_id
AND DATE(DT.article_date) BETWEEN DATE_ADD(NOW(),INTERVAL -1 MONTH ) AND NOW()
GROUP BY KD.keyword_id
ORDER BY 횟수 DESC
LIMIT 100;
''')

week_hot_keyword = cur.fetchall()

cur.execute(f'''
SELECT K.keyword_name day_keywords, COUNT(K.keyword_id) 횟수
FROM default_db.keyword_detail KD, default_db.keyword K, default_db.default_tb DT
WHERE KD.keyword_id = K.keyword_id
AND KD.article_id = DT.article_id
AND DATE(DT.article_date) = "{yesterday}"
GROUP BY KD.keyword_id
ORDER BY 횟수 DESC
LIMIT 50;
''')

if not cur.rowcount:
    day_keywords = 0
else:
    day_keywords = cur.fetchall()

day_list, day_count = zip(*day_keywords[:20])
max_cnt = (max(day_count)//10)*10 + 10

font_name = fm.FontProperties(fname='./static/fonts/NanumBarunpenB.ttf').get_name()
rc('font', family=font_name)

kws = ['삼성전자', '현대차', '기아', '카카오', '네이버']
integration_node = []
integration_edge = []
loaded_model = Word2Vec.load('trained_model')

for i, kw in enumerate(kws):
    relation_words = loaded_model.wv.most_similar(kw)
    relation_words = dict(relation_words)
    node_dataset = [{'id': idx, 'label': relation_word} for idx, relation_word in enumerate(relation_words, i*11+1)]
    node_dataset.append({'id': i*11, 'label': kw})
    integration_node.extend(node_dataset)

    edge_dataset = [{'from': i*11, 'to': idx, 'width': 1} for idx in range(11*i+1, len(node_dataset)+i*11)]
    integration_edge.extend(edge_dataset)


@app.route('/')
def index():
    return render_template('index.html',
                           table=enumerate(week_hot_keyword, 1),
                           today=today,
                           yesterday=yesterday,
                           week_ago=week_ago.strftime('%Y-%m-%d'),
                           day_list=list(day_list),
                           day_count=list(day_count),
                           max_cnt=max_cnt,
                           node_dataset=integration_node,
                           edge_dataset=integration_edge)


@app.route('/week_keywords')
def week_keywords():
    stock = request.args.get('sv', '005930')
    keyword = request.args.get('keyword')
    count = []

    for day in week[::-1]:
        cur.execute(f'''
        SELECT COUNT(K.keyword_id) 횟수
        FROM default_db.keyword_detail KD, default_db.keyword K, default_db.default_tb DT
        WHERE KD.keyword_id = K.keyword_id
        AND KD.article_id = DT.article_id
        AND K.keyword_name = "{keyword}"
        AND DATE(DT.article_date) = "{day}"
        GROUP BY KD.keyword_id;
        ''')

        if not cur.rowcount:
            f = 0
        else:
            f = cur.fetchone()[0]
        count.append(f)

    df = web.DataReader(stock, 'naver', week_ago - datetime.timedelta(days=2), now + datetime.timedelta(days=2))
    df = df.Close.asfreq(freq='D', method='bfill')
    df = df.reset_index()
    df = df[(df.Date >= week_ago.strftime('%Y-%m-%d')) & (df.Date < now.strftime('%Y-%m-%d'))]

    date, close = df.Date.tolist(), df.Close.tolist()
    date = [d.strftime('%Y-%m-%d') for d in date]
    close = [int(c) for c in close]
    min_ = int(min(close) * 0.99)
    max_ = int(max(close) * 1.01)

    return json.dumps({'week':week, 'count':count, 'date': date, 'close': close, 'max_': max_, 'min_': min_})


@app.route('/wordcloud_week')
def wordcloud_week():
    kw_dictionary = dict((a, b) for a, b in week_hot_keyword)
    wc = WordCloud(font_path= "./static/fonts/NanumBarunpenB.ttf", width=1920, height=1080, background_color="white")
    wc = wc.generate_from_frequencies(kw_dictionary)
    wc.to_file('./static/assets/img/wc_week.jpg')

    return render_template('wordcloud_week.html')


@app.route('/wordcloud_day')
def wordcloud_day():
    kw_dictionary = dict((a, b) for a, b in day_keywords)
    wc = WordCloud(font_path= "./static/fonts/NanumBarunpenB.ttf", width=1920, height=1080, background_color="white")
    wc = wc.generate_from_frequencies(kw_dictionary)
    wc.to_file('./static/assets/img/wc_day.jpg')

    return render_template('wordcloud_day.html')


if __name__ == '__main__':
    app.run(host='localhost', port=8078, debug=True)