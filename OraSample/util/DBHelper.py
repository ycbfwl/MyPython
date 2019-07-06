# -*- coding: utf-8 -*-
__author__ = 'hannibal'

from urllib import request
import json
from datetime import datetime, timedelta
import time
import cx_Oracle
import sys
import io
import pymysql
import re

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf8')


# 获取数据
def get_data(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1',
        'Accept-Encoding': 'gizp,defale',
        'Accept-Language': 'zh-CN,zh;q=0.9'
    }
    req = request.Request(url, headers=headers)

    response = request.urlopen(req)
    if response.getcode() == 200:
        return response.read()


# 处理数据
def parse_data(html):
    data = json.loads(html)['cmts']
    comments = []
    for item in data:
        comment = {
            'id': item['id'],
            'nickName': item['nickName'],
            'cityName': item['cityName'] if 'cityName' in item else '',  # 处理cityName不存在情况
            'content': item['content'].replace('\n', ' '),  # 处理评论内容换行的情况
            'score': item['score'],
            'startTime': item['startTime']
        }
        comments.append(comment)
    return comments


def save_to_db(id, nickname, cityname, content, score, starttime):
    conn = cx_Oracle.connect('hannibal/hannibal123@192.168.31.22:1521/orcl')
    cursor = conn.cursor()
    # nickname = 'nickname'
    # cityname = 'cityname'
    # content = 'content'
    sqlstr = "insert into cat_comments(id_c,nickName_c,cityName_c,content_c,score_c,tartTime_c) values (:1,:2,:3,:4,:5,:6)"
    # print(sqlstr)
    result = cursor.execute(sqlstr, [id, nickname, cityname, content, score, starttime])
    conn.commit()
    cursor.close
    conn.close
    return result


def save_to_db2(id, nickname, cityname, content, score, starttime):
    config = {
        'host': '192.168.31.24',
        'port': 3306,
        'user': 'hannibal',
        'password': '1234',
        'database': 'python',
        'charset': 'utf8'
    }

    # 获取连接
    conn = pymysql.connect(**config)

    # 获取游标，相当于java中的Statement
    cursor = conn.cursor()

    # 执行sql
    sql = '''
        insert into cat_comments(id_c,nickName_c,cityName_c,content_c,score_c,tartTime_c)
        values 
          (%s,%s,%s,%s,%s,%s)  
    '''
    num = cursor.execute(sql, [id, nickname, cityname, content, score, starttime])  # 为占位符%s赋值
    print(num)

    # 提交事务
    conn.commit()

    # 关闭资源
    cursor.close()
    conn.close()


def clean_zh_text(text):
    # keep English, digital and Chinese
    comp = re.compile('[^A-Z^a-z^0-9^\u4e00-\u9fa5]')
    return comp.sub('', text)


# 存储数据到文本文件
def save_to_txt():
    start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 当前时间
    end_time = '2018-08-10 00:00:00'  # 结束时间
    while start_time > end_time:
        url = 'http://m.maoyan.com/mmdb/comments/movie/1203084.json?_v_=yes&offset=0&startTime=' + start_time.replace(
            ' ', '%20')
        try:
            html = get_data(url)
        except:
            time.sleep(1)
            html = get_data(url)
        else:
            time.sleep(0.1)

        comments = parse_data(html)
        print(comments)

        start_time = comments[-1]['startTime']  # 末尾评论时间
        start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S') - timedelta(seconds=1)  # 向前减１秒，防止获取到重复数据
        start_time = datetime.strftime(start_time, '%Y-%m-%d %H:%M:%S')

        for item in comments:
            # print(type(item['nickName']))
            print(str(item['id']), item['nickName'], item['cityName'], item['content'], str(item['score']),
                  item['startTime'])
            save_to_db(str(item['id']), clean_zh_text(item['nickName']), item['cityName'],
                       clean_zh_text(item['content']), str(item['score']),
                       item['startTime'])


if __name__ == '__main__':
    # url = 'http://m.maoyan.com/mmdb/comments/movie/1203084.json?_v_=yes&offset=15&startTime=2018-09-01%2011%3A10%3A00'
    # comments = parse_data(get_data(url))
    # print(comments)
    save_to_txt()
