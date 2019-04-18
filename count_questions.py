from slacker import Slacker
import pymysql
import pprint as pp
import pandas
from os import environ
from datetime import datetime
from pymongo import MongoClient


def old_quant(item):
    x = coll.find_one({}, sort=[('_created', -1)])
    old_quant = x[item['banco']]
    return old_quant


def diff(item):
    x = item['quant'] - item['old_quant']
    if x > 0:
        x = '+{}'.format(x)
    return x


def conn():
    client = MongoClient('mongodb://localhost', 27017)
    db = client.log
    coll = db['log_quant']
    return coll


servers = {
    'localhost1': [
        'CIVIL_PROD',
        'CONCURSO',
        'PRF',
        'TRIBUNAIS'
    ],
    'localhost2': [
        'CONTROLE_PROD',
        'DIPLOMACIA_PROD',
        'LEG_PROD'
    ],
    'localhost3': [
        'FISCAL',
        'JURIDICA'
    ],
    'localhost4': [
        'POLICIAL',
    ],
    'localhost5': [
        'ADM',
        'BANCARIO',
        'CONTABILIDADE',
        'INFORMATICA',
        'INSS'
    ]
}

dados = {}

for k, v in servers.items():
    connection = pymysql.connect(
        host=k,
        user='root',
        password='',
        cursorclass=pymysql.cursors.DictCursor,
        charset='utf8',
    )
    cursor = connection.cursor()
    for db in v:
        cs = pymysql.connect(
            host=k,
            user='root',
            password='admin',
            cursorclass=pymysql.cursors.DictCursor,
            charset='utf8',
            db=db
        ).cursor()
        sql_count = """SELECT count(*) as `quant` FROM `question`"""
        cs.execute(sql_count)
        count_questions = cs.fetchone()
        count_questions = count_questions['quant']

        h = {db: count_questions}

        dados.update(h)

coll = conn()
pandas.set_option('display.max_columns', None)
df = pandas.DataFrame()
df['banco'] = dados.keys()
df['quant'] = dados.values()
df['old_quant'] = df.apply(old_quant, axis=1)
df['diff'] = df.apply(diff, axis=1)
print(df)
dados.update({'_created': datetime.now()})
coll.insert_one(dados)
open('report.csv', 'w+').write(df.to_csv())
df = df.sort_values('banco')
df.reset_index(drop=True, inplace=True)

token = '<YOU TOKEN FROM SLACK BOT>'
channel = '#chanel'
job_url = environ.get('CI_JOB_URL', '')
slack = Slacker(token)
slack.chat.post_message(channel, '```{}```'.format(repr(df)))
if job_url:
    message = job_url + '/artifacts/download'
    slack.chat.post_message(channel, message)
