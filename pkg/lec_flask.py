import json
import datetime
import random

from flask import Flask, render_template, request
import pymysql
import pytagcloud
import pandas_datareader.data as web

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
AND DATE(DT.article_date) BETWEEN DATE_ADD(NOW(),INTERVAL -1 WEEK ) AND NOW()
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


@app.route('/')
def index():
    return render_template('index.html',
                           table=enumerate(week_hot_keyword, 1),
                           today=today,
                           yesterday=yesterday,
                           week_ago=week_ago.strftime('%Y-%m-%d'),
                           day_list=list(day_list),
                           day_count=list(day_count),
                           max_cnt=max_cnt)


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

    df = web.DataReader(stock, 'naver', week_ago, now)
    df = df.Close.asfreq(freq='D', method='bfill')
    df = df.reset_index()
    date, close = df.Date.tolist(), df.Close.tolist()
    date = [d.strftime('%Y-%m-%d') for d in date]
    close = [int(c) for c in close]
    min_ = int(min(close) * 0.99)
    max_ = int(max(close) * 1.01)

    return json.dumps({'week':week, 'count':count, 'date': date, 'close': close, 'max_': max_, 'min_': min_})


@app.route('/wordcloud_week')
def wordcloud_week():
    r = lambda: random.randint(0,255)
    color = lambda: (r(), r(), r())
    week_hot_keyword_ = sorted(week_hot_keyword, key=lambda x: x[1], reverse=True)
    m = max(list(c for n, c in week_hot_keyword_))
    tags = [{ 'color': color(), 'tag': n, 'size': int(c/m*300) } for n, c in week_hot_keyword_[:50]]
    pytagcloud.create_tag_image(tags, './static/assets/img/wordcloud_week.jpg', fontname='NanumBarunpenB', size=(1920, 1080))
    return render_template('wordcloud_week.html')


@app.route('/wordcloud_day')
def wordcloud_day():
    r = lambda: random.randint(0,255)
    color = lambda: (r(), r(), r())
    day_keywords_ = sorted(day_keywords, key=lambda x: x[1], reverse=True)
    m = max(list(c for n, c in day_keywords_))
    tags = [{ 'color': color(), 'tag': n, 'size': int(c/m*200) } for n, c in day_keywords_]
    pytagcloud.create_tag_image(tags, './static/assets/img/wordcloud_day.jpg', fontname='NanumBarunpenB', size=(1920, 1080))
    return render_template('wordcloud_day.html')


if __name__ == '__main__':
    app.run(host='localhost', port=8078, debug=True)