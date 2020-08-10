# -*- coding: utf-8 -*-
import requests
import lxml
# from lxml import etree
from lxml import html
etree=html.etree
from urllib import parse
import os
import traceback
from functools import reduce
import json
import re
import time
import urllib.parse
from datetime import datetime
import random
from bs4 import BeautifulSoup
import unicodedata
import random
import hashlib
from faker import Factory
import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.helpers import bulk
es_client = Elasticsearch(hosts=[{'host':'39.97.190.184', 'port':'9200'}],http_auth=('elastic', '008800'))
es = Elasticsearch([{'host': '127.0.0.1','port': 9200}])

def get_ua():
    f = Factory.create()
    ua = f.user_agent()
    return ua
def get_headers():
    f = Factory.create()
    ua = f.user_agent()
    headers = {
        'User-Agent': ua,
    }
    return headers
class news():
    def __init__(self):
        self.ua=get_ua()
        self.headers=get_headers()

    def get_md5(self,url):
        if isinstance(url, str):
            url = url.encode("utf-8")
        m = hashlib.md5()
        m.update(url)
        return m.hexdigest()

    def nytimes_tail(self,tail_href, label_name):
        print('开始执行')
        nytime_json = {}
        nytime_json['inssue_tag'] = label_name
        nytime_json['website'] = 'nytimestoday'
        nytime_json['daily_or_week'] = 'nytimestoday_daily'
        nytime_json['url'] = tail_href
        url_encryption = self.get_md5(tail_href)
        nytime_json['url_encryption'] = url_encryption
        dsl = {
            "query": {
                "match": {
                    "_id": url_encryption,
                },
            }
        }
        results = es_client.search(index='news2020', body=dsl)
        res = results['hits']['hits']
        if not res:
            while True:
                try:
                    time.sleep(random.randrange(1, 5))
                    proxies = {
                        'http': 'http://127.0.0.1:24001',
                        'https': 'http://127.0.0.1:24001',
                    }
                    ua=get_ua()
                    payload = {}
                    headers = {
                        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                        'accept-encoding': 'gzip, deflate, br',
                        'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'cache-control': 'max-age=0',
                        'sec-fetch-dest': 'document',
                        'sec-fetch-mode': 'navigate',
                        'sec-fetch-site': 'same-origin',
                        'sec-fetch-user': '?1',
                        'upgrade-insecure-requests': '1',
                        'user-agent': ua,
                    }
                    response = requests.request("GET", tail_href, verify = False, headers=headers, proxies=proxies, data=payload,
                                                timeout=30)
                    myhtml = etree.HTML(response.text)
                    if myhtml.xpath('//script[@type="application/ld+json"]/text()')[0]:
                        break
                    else:
                        continue
                except Exception as e:
                    print(e)
                    continue
            head_data = myhtml.xpath('//script[@type="application/ld+json"]/text()')[0]
            head_data_json = json.loads(head_data)
            h_title = head_data_json['headline']
            print('h_title是：{h_title}'.format(h_title=h_title))
            nytime_json['title'] = h_title
            h_content = head_data_json['description']
            nytime_json['head_content'] = h_content
            print('h_content是：{h_content}'.format(h_content=h_content))
            author = []
            authors = head_data_json['author']
            if type(authors) == list:
                for au in authors:
                    author_name = au['name']
                    author.append(author_name)
            nytime_json['authors'] = author
            print('author是：{author}'.format(author=author))
            datepublished = head_data_json['datePublished']
            nytime_json['datepublished'] = datepublished
            print('publistdate是：{publistdate}'.format(publistdate=datepublished))
            
            contents = []
            if myhtml.xpath(
                    '//section[@itemprop="articleBody"]/child::*|//div[@class="g-story g-freebird g-max-limit "]/child::*|//div[@class="g-intro"]/child::*'):
                content_section = myhtml.xpath(
                    '//section[@itemprop="articleBody"]/child::*|//div[@class="g-story g-freebird g-max-limit "]/child::*|//div[@class="g-intro"]/child::*')
                for c_s in range(len(content_section)):
                    if content_section[c_s].xpath('./div/p/text()|./div/p/a/text()|./text()|./a/text()|./p/text()'):
                        
                        if content_section[c_s].xpath('./div/p/a/text()'):
                            
                            temp = content_section[c_s].xpath('./div')[0]
                            for s in temp:
                                c_s_content = ''.join(s.xpath('./text()|./a/text()|./p/text()'))
                                contents.append(c_s_content)
                        else:
                            c_s_content = '\n'.join(content_section[c_s].xpath('./div/p/text()|./div/p/a/text()|./text()|./a/text()|./p/text()'))
                            if c_s_content:
                                contents.append(c_s_content)
                        continue
                    else:
                        continue
            contents_search = '###'.join(contents)
            nytime_json['contents'] = contents
            nytime_json['contents_search'] = contents_search
            print('contents：{contents}'.format(contents=contents))
            try:
                es_client.index(index="news2020", doc_type="_doc", id=url_encryption, body=nytime_json)
                print("保存数据成功")
            except Exception as e:
                print(e)
    def nytimes_today(self,year,month,day):
        url = "https://www.nytimes.com/issue/todayspaper/{year}/{month}/{day}/todays-new-york-times".format(year = year,month = month, day = day)
        print (url)
        while True:
            print('开始执行NYT')
            time.sleep(random.randrange(1,5))
            try:
                proxies = {
                    'http': 'http://127.0.0.1:24001',
                    'https': 'http://127.0.0.1:24001',
                }
                payload = {}
                ua=get_ua()
                headers = {
                  'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                  'accept-encoding': 'gzip, deflate, br',
                  'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
                  'cache-control': 'max-age=0',
                  'referer': 'https://www.nytimes.com/',
                  'sec-fetch-dest': 'document',
                  'sec-fetch-mode': 'navigate',
                  'sec-fetch-site': 'same-origin',
                  'sec-fetch-user': '?1',
                  'upgrade-insecure-requests': '1',
                  'user-agent': ua,
                }
                response = requests.request("GET", url, verify = False, headers=headers, proxies=proxies,data = payload,timeout=30)
                if re.findall(u'window.__preloadedData = ({.+});', response.text)[0]:
                    break
                else:
                    continue
            except Exception as e:
                print(e)
                continue
        #通过正则表达式解析script中的内容
        content = re.findall(u'window.__preloadedData = ({.+});', response.text)[0]
        content_json=json.loads(content)
        initial_keys=list(content_json['initialState'].keys())
        for i_k in initial_keys:
            if 'LegacyCollection:' in i_k:
                if 'label' in content_json['initialState'][i_k].keys():
                    label_name=content_json['initialState'][i_k]['label']
                    if label_name=='The Front Page':
                        relations=content_json['initialState'][i_k]['relations']
                        for rel in relations:
                            rel_id=rel['id']
                            article_id=content_json['initialState'][rel_id]['asset']['id']
                            article_url=content_json['initialState'][article_id]['url']
                            self.nytimes_tail(article_url,label_name)
                            article_summary=content_json['initialState'][article_id]['summary']
                            article_title_id=content_json['initialState'][article_id]['headline']['id']
                            article_title=content_json['initialState'][article_title_id]['default']
                    elif label_name=='International':
                        relations=content_json['initialState'][i_k]['relations']
                        for rel in relations:
                            rel_id=rel['id']
                            article_id=content_json['initialState'][rel_id]['asset']['id']
                            article_url=content_json['initialState'][article_id]['url']
                            self.nytimes_tail(article_url, label_name)
                            article_summary=content_json['initialState'][article_id]['summary']
                            article_title_id=content_json['initialState'][article_id]['headline']['id']
                            article_title=content_json['initialState'][article_title_id]['default']
                    elif label_name=='National':
                        relations=content_json['initialState'][i_k]['relations']
                        for rel in relations:
                            rel_id=rel['id']
                            article_id=content_json['initialState'][rel_id]['asset']['id']
                            article_url=content_json['initialState'][article_id]['url']
                            self.nytimes_tail(article_url, label_name)
                            article_summary=content_json['initialState'][article_id]['summary']
                            article_title_id=content_json['initialState'][article_id]['headline']['id']
                            article_title=content_json['initialState'][article_title_id]['default']
                    elif label_name=='Business Day':
                        relations=content_json['initialState'][i_k]['relations']
                        for rel in relations:
                            rel_id=rel['id']
                            article_id=content_json['initialState'][rel_id]['asset']['id']
                            article_url=content_json['initialState'][article_id]['url']
                            self.nytimes_tail(article_url, label_name)
                            article_summary=content_json['initialState'][article_id]['summary']
                            article_title_id=content_json['initialState'][article_id]['headline']['id']
                            article_title=content_json['initialState'][article_title_id]['default']
                    elif label_name=='Science Times':
                        relations=content_json['initialState'][i_k]['relations']
                        for rel in relations:
                            rel_id=rel['id']
                            article_id=content_json['initialState'][rel_id]['asset']['id']
                            article_url=content_json['initialState'][article_id]['url']
                            self.nytimes_tail(article_url, label_name)
                            article_summary=content_json['initialState'][article_id]['summary']
                            article_title_id=content_json['initialState'][article_id]['headline']['id']
                            article_title=content_json['initialState'][article_title_id]['default']

if __name__ == '__main__':
    spider = news()
    year = "2020"
    month = '02'
    for day in range(1,30): 
        if day < 10:
            spider.nytimes_today(year,month,"0"+str(day)) 
        else:   
            spider.nytimes_today(year,month,str(day))



