import re

import pandas as pd
import pymysql
from konlpy.tag import Komoran
from apyori import apriori
import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import rc
import matplotlib.font_manager as fm
import matplotlib


matplotlib.font_manager._rebuild()

font_name = fm.FontProperties(fname="C:/Windows/Fonts/cambria.ttc").get_name()
rc('font', family=font_name)

plt.rc('font', family='Malgun Gothic')
pd.options.display.max_rows = 1000
pd.options.display.max_columns = 1000

komporan = Komoran()

conn = pymysql.connect(host='localhost', port=3306, user='root', db='default_db',
                       password='qw75718856**', charset='utf8')

cur = conn.cursor()

cur.execute(f'''
SELECT article_title 
FROM default_tb DT;
''')

titles = cur.fetchall()
titles = [title[0] for title in titles]
tesla = [title for title in titles if '테슬라' in title]


dataset = []

for title in tesla:
    dataset.append(komporan.nouns(re.sub('[^가-힣a-zA-Z\s]', '', title)))


result = list(apriori(dataset, min_support=0.01))
df = pd.DataFrame(result)
df['length'] = df['items'].apply(lambda x: len(x))
df = df[(df.length == 2) & (df.support > 0.01)].sort_values(by='support', ascending=False)
print(df.head())

G = nx.Graph()
ar = (df['items'])
G.add_edges_from(ar)

pr = nx.pagerank(G)
nsize = np.array([v for v in pr.values()])
nsize = 2000 * (nsize - min(nsize)) / (max(nsize) - min(nsize))

pos = nx.random_layout(G)

plt.figure(figsize=(16, 12))
plt.axis('off')
nx.draw_networkx(G, font_family='Malgun Gothic', font_size=16,
                 pos=pos, node_color=list(pr.values()), node_size=nsize,
                 alpha=0.7, edge_color='.5', cmap=plt.cm.YlGn)
plt.savefig('./IMG01.png', bbox_inches='tight')


