import b
import requests
from bs4 import BeautifulSoup
import re
from collections import defaultdict
import sqlite3
import urllib.request
import os

to_do = [
         # 'Reimu_Hakurei', 'Marisa_Kirisame', 'Sakuya_Izayoi', 'Youmu_Konpaku',
         # 'Yukari_Yakumo', 'Alice_Margatroid', 'Remilia_Scarlet', 'Yuyuko_Saigyouji',
         # 'Flandre_Scarlet', 'Patchouli_Knowledge', 'Hong_Meiling',
         # 'Kaguya_Houraisan', 'Eirin_Yagokoro', 'Reisen_Udongein_Inaba',
         # 'Ran_Yakumo', 'Chen', 'Prismriver_Sisters',
         # 'Fujiwara_no_Mokou', 'Keine_Kamishirasawa', 'Suika_Ibuki',
         # 'Suwako_Moriya', 'Kanako_Yasaka', 'Sanae_Kochiya',
         # 'Aya_Shameimaru', 'Komachi_Onozuka', 'Nitori_Kawashiro',
         # 'Tenshi_Hinanawi', 'Iku_Nagae',
         # 'Utsuho_Reiuji', 'Rin_Kaenbyou',
         # 'Koishi_Komeiji', 'Satori_Komeiji',
         # 'Byakuren_Hijiri', 'Shou_Toramaru',
         # 'Nue_Houjuu', 'Kogasa_Tatara',
         # 'Nazrin', 'Minamitsu_Murasa',
         # 'Toyosatomimi_no_Miko', 'Mamizou_Futatsuiwa',
         # 'Cirno', 'Three_Fairies',
         # 'Mononobe_no_Futo', 'Ichirin_Kumoi',
         # 'Shinmyoumaru_Sukuna', 'Hata_no_Kokoro', 'Mystia_Lorelei',
         # 'Seiga_Kaku', 'Seija_Kijin',
         'Team_Supports', 'Team_Events', 'General'
         ]
base = 'https://en.touhouwiki.net'


header = {
  "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
  "X-Requested-With": "XMLHttpRequest"
}
opener = urllib.request.build_opener()
opener.addheaders = [('User-agent', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36')]
urllib.request.install_opener(opener)

for character in to_do:
    if os.chdir(f'./img/{character}'):
        print(f'path : ./img/{character} already exists')
        continue
    os.mkdir(f'./img/{character}')
    url = f'https://en.touhouwiki.net/wiki/Rumbling_Spell_Orchestra/{character}'
    soup = BeautifulSoup(requests.get(url, headers=header).text.encode('utf-8'), 'lxml')
    body = soup.find('body')
    imgs = body.find_all('img')
    for img in imgs:
        img_url = base + img.attrs['src']
        img_name = img_url.split('/')[-1]
        if img_name == 'cc-by-sa.png' or img_name == 'poweredby_mediawiki_88x31.png':
            continue

        regex_name = re.findall(r'([vp]*[0-9]+[a-zA-Z]*\..+)$', img_name)[0]
        regex_name = regex_name.replace('v', '5')
        regex_name = re.sub(r'^[p]', '6', regex_name)
        urllib.request.urlretrieve(img_url, f'./img/{character}/{regex_name}')


# conn = sqlite3.connect('test.db')
# cur = conn.cursor()


# for character in to_do:
#     r = requests.get(f'https://en.touhouwiki.net/wiki/Rumbling_Spell_Orchestra/{character}', headers=header)
#
#     soup = BeautifulSoup(r.text.encode('utf-8'), 'lxml')
#
#     heads = soup.select('h2 > span.mw-headline')
#     tables = soup.find_all('table')
#
#     for table in tables[:-3]:
#         trs = table.select('tbody > tr')
#         d = defaultdict(str)
#         for idx, tr in enumerate(trs):
#             try:
#                 if idx == 0:
#                     category = tr.select_one('th').string.strip().replace(':', '').replace('：', '')
#                     card_no = re.search(r'\s([a-zA-Z0-9]+)', category).group(0)
#                     content = tr.contents[-1].get_text(' ', strip=True)
#                     d['No'] = card_no
#                     d['Name'] = content
#                 else:
#                     category = tr.select_one('th').string.strip().replace(':', '').replace('：', '')
#                     content = tr.select_one('td').get_text(' ', strip=True)
#                     d[category] = content
#
#
#             except Exception as e:
#                 print(e)
#
#
#
#
#         print(d)
#         if 'Designations' in d.keys():
#             cur.execute('''
#             CREATE TABLE IF NOT EXISTS character
#             (id TEXT, character TEXT, name TEXT, version TEXT, artist TEXT, designations TEXT, stats TEXT, special TEXT)
#             ''')
#             cur.execute(f'''
#             INSERT INTO character VALUES(?, ?, ?, ?, ?, ?, ?, ?);
#             ''', (d["No"], character, d["Name"], d["Version"], d["Artist"], d["Designations"], d["Stats"], d["Special"]))
#
#         elif 'Spell Type' in d.keys():
#             cur.execute('''
#             CREATE TABLE IF NOT EXISTS spell
#             (id TEXT, character TEXT, name TEXT, version TEXT, artist TEXT, requirements TEXT, spell_type TEXT, stats TEXT, special TEXT)
#             ''')
#             cur.execute(f'''
#             INSERT INTO spell VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);
#             ''', (d["No"], character, d["Name"], d["Version"], d["Artist"], d["Requirements"], d["Spell Type"], d["Stats"], d["Special"]))
#
#         elif 'Support Type' in d.keys():
#             cur.execute('''
#             CREATE TABLE IF NOT EXISTS support
#             (id TEXT, character TEXT, name TEXT, version TEXT, artist TEXT, requirements TEXT, support_type TEXT, special TEXT)
#             ''')
#             cur.execute(f'''
#             INSERT INTO support VALUES(?, ?, ?, ?, ?, ?, ?, ?);
#             ''', (d["No"], character, d["Name"], d["Version"], d["Artist"], d["Requirements"], d["Support Type"], d["Special"]))
#
#         elif 'Effect' in d.keys():
#             cur.execute('''
#             CREATE TABLE IF NOT EXISTS event
#             (id TEXT, character TEXT, name TEXT, version TEXT, artist TEXT, requirements TEXT, phase TEXT, effect TEXT)
#             ''')
#             cur.execute(f'''
#             INSERT INTO event VALUES(?, ?, ?, ?, ?, ?, ?, ?);
#             ''', (d["No"], character, d["Name"], d["Version"], d["Artist"], d["Requirements"], d["Phase"], d["Effect"]))
#         conn.commit()
#
#         print('----------------------------------------------------------------------------------------------')
#
# #character stats 분리
#
# cur.execute(f'''
# SELECT stats FROM character;
# ''')
# stats = cur.fetchall()
#
#
# cur.execute(f'''
# ALTER TABLE character ADD COLUMN hit_points INT;
# ''')
#
# cur.execute(f'''
# ALTER TABLE character ADD COLUMN evasion_value INT;
# ''')
#
# cur.execute(f'''
# ALTER TABLE character ADD COLUMN border_value INT;
# ''')
#
# for idx, item in enumerate(stats, 1):
#     stat = re.findall(r'([0-9]+)', item[0])
#     cur.execute(f'''
#     UPDATE character SET hit_points = {stat[0]} WHERE rowid = {idx};
#     ''')
#     cur.execute(f'''
#     UPDATE character SET evasion_value = {stat[1]} WHERE rowid = {idx};
#     ''')
#     cur.execute(f'''
#     UPDATE character SET border_value = {stat[2]} WHERE rowid = {idx};
#     ''')
#     conn.commit()

#spell stats 분리

# cur.execute(f'''
# SELECT stats FROM spell;
# ''')
# stats = cur.fetchall()


# cur.execute(f'''
# ALTER TABLE spell ADD COLUMN attack_value INT;
# ''')
#
# cur.execute(f'''
# ALTER TABLE spell ADD COLUMN intercept_value INT;
# ''')
#
# cur.execute(f'''
# ALTER TABLE spell ADD COLUMN hit_value INT;
# ''')
#
# cur.execute(f'''
# ALTER TABLE spell ADD COLUMN basic_abilities TEXT;
# ''')

# for idx, item in enumerate(stats, 1):
#     stat = re.findall('Basic Abilities:\s(.+)', item[0]) or ['']
#     print(stat)
    # cur.execute(f'''
    # UPDATE spell SET attack_value = {stat[0][0]} WHERE rowid = {idx};
    # ''')
    # cur.execute(f'''
    # UPDATE spell SET intercept_value = {stat[1][0]} WHERE rowid = {idx};
    # ''')
    # cur.execute(f'''
    # UPDATE spell SET hit_value = {stat[2][0]} WHERE rowid = {idx};
    # ''')
    # cur.execute(f'''
    # UPDATE spell SET basic_abilities = '{stat[0]}' WHERE rowid = {idx};
    # ''')
    # conn.commit()


# cur.execute(f'''
# SELECT * FROM spell;
# ''')
# a = cur.fetchall()
# print(a)
#
# cur.execute(f'''
# SELECT * FROM event;
# ''')
# a = cur.fetchall()
# print(a)
#
# cur.execute(f'''
# SELECT * FROM support;
# ''')
# a = cur.fetchall()
# print(a)

# cur.execute('''
# SELECT id from character;
# ''')
#
# ids = cur.fetchall()
# for idx, id in enumerate(ids):
#     print(int(id[0]))
#     cur.execute(f'''
#     INSERT INTO T_CARD VALUES (?, ?)''', (int(id[0]), 'C'))


# cur.execute('''
# SELECT id from event;
# ''')
#
# ids = cur.fetchall()
# for idx, id in enumerate(ids):
#     try:
#         print(int(id[0].replace('V', '5').replace('P', '6')))
#         cur.execute(f'''
#         INSERT INTO T_CARD VALUES (?, ?)''', (int(id[0].replace('V', '5').replace('P', '6')), 'E'))
#     except:
#         print('*****', int(id[0]), '*****')

# cur.execute('''
# SELECT id from spell;
# ''')
#
# ids = cur.fetchall()
# for idx, id in enumerate(ids):
#     try:
#         print(int(id[0].replace('V', '5').replace('P', '6')))
#         cur.execute(f'''
#         INSERT INTO T_CARD VALUES (?, ?)''', (int(id[0].replace('V', '5').replace('P', '6')), 'S'))
#     except:
#         print('*****', int(id[0]), '*****')

# cur.execute('''
# SELECT id from support;
# ''')

# ids = cur.fetchall()
# for idx, id in enumerate(ids):
#     try:
#         print(int(id[0].replace('V', '5').replace('P', '6')))
#         cur.execute(f'''
#         INSERT INTO T_CARD VALUES (?, ?)''', (int(id[0].replace('V', '5').replace('P', '6')), 'S'))
#     except:
#         print('*****', int(id[0]), '*****')
#
# conn.commit()

# cur.close()
# conn.close()






