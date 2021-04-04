import json
import datetime

from flask import Flask, render_template, request
import pandas_datareader.data as web
from wordcloud import WordCloud

import keyword_data as kd


app = Flask(__name__, template_folder='templates')

@app.route('/')
def index():
    table = enumerate(kd.get_month_keyword(), 1)
    nodes, edges = kd.keyword_network(['삼성전자', '현대차', '기아', '카카오', '네이버', '코로나'])
    day_list, day_count = kd.get_week_list()
    max_cnt = (max(day_count)//10)*10 + 10

    return render_template('index.html', table=table, nodes=nodes, edges=edges,
                           day_count=list(day_count), day_list=list(day_list), max_cnt=max_cnt)


@app.route('/month_chart')
def month_chart():
    stock = request.args.get('sv', '005930')
    keyword = request.args.get('keyword')
    now = datetime.datetime.now()
    month_date = [(now - datetime.timedelta(days=x)).strftime('%Y-%m-%d') for x in reversed(range(1, 31))]

    df = web.DataReader(stock, 'naver', month_date[0], month_date[-1])
    df = df.Close.asfreq(freq='D', method='bfill')
    count = kd.get_day_by_day_count(keyword, df.index)
    df = df.reset_index()

    date, close = df.Date.tolist(), df.Close.tolist()
    close = [int(c) for c in close]
    min_ = int(min(close) * 0.99)
    max_ = int(max(close) * 1.01)

    return json.dumps({'month_date': month_date, 'count': count, 'close': close, 'max_': max_, 'min_': min_})


@app.route('/wordcloud_month')
def wordcloud_month():
    month_hot_keyword = kd.get_month_keyword()
    kw_dictionary = dict((a, b) for a, b in month_hot_keyword)
    wc = WordCloud(font_path= "./static/fonts/NanumBarunpenB.ttf", width=1920, height=1080, background_color="white")
    wc = wc.generate_from_frequencies(kw_dictionary)
    wc.to_file('./static/assets/img/wc_month.jpg')

    return render_template('wordcloud_month.html')


@app.route('/wordcloud_week')
def wordcloud_week():
    week_hot_keyword = kd.get_week_keyword()
    kw_dictionary = dict((a, b) for a, b in week_hot_keyword)
    wc = WordCloud(font_path= "./static/fonts/NanumBarunpenB.ttf", width=1920, height=1080, background_color="white")
    wc = wc.generate_from_frequencies(kw_dictionary)
    wc.to_file('./static/assets/img/wc_week.jpg')

    return render_template('wordcloud_week.html')


if __name__ == '__main__':
    app.run(host='localhost', port=8078, debug=True)