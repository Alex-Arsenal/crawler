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
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

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

    def getText(self,group):
        text = []
        proxies = {
                    'http': 'http://127.0.0.1:24000',
                    'https': 'http://127.0.0.1:24000',
                }
        for s in group:
            if str(s).startswith('<p'):
                temp = s.get_text().encode('utf-8')
                if len(str(temp).split(" ")) <= 1:
                    continue
                text.append(str(s.get_text().encode('utf-8')))
            else:
                cc_img_src = s['src']
                if cc_img_src.startswith('http'):
                    print (cc_img_src)
                    cc_img_src_new = parse.urljoin(cc_img_src, '800x-1.jpg')
                    res = re.match('.*/(.*?\.\w+).*?', cc_img_src_new).group(1)
                    image_image = cc_img_src_new.split('/')[-5] + '_' + cc_img_src_new.split('/')[
                        -4] + '_' + cc_img_src_new.split('/')[-3] + '_' + cc_img_src_new.split('/')[
                                    -2] + '_' + res
                    root = ".\\image\\"
                    path = root + image_image
                    if not os.path.exists(root):
                        os.mkdir(root)
                    if not os.path.exists(path):
                        with open(path, 'wb') as f:
                            while True:
                                try:
                                    if requests.get(url=cc_img_src_new,verify = False,proxies=proxies,timeout=30
                                                ).content:
                                        f.write(requests.get(url=cc_img_src_new, verify = False,proxies=proxies, timeout=30
                                                    ).content)
                                        break
                                    else:
                                        continue
                                except Exception as e:
                                    print(e)
                                    continue
                        f.close()
                    cc_img_src = 'https://trends.lenovoresearch.cn/' + image_image
                    if cc_img_src not in text:
                        text.append(cc_img_src)
        return "\n".join(text)

    def bloomberg_index(self):
        print('开始执行')
        index_urls=[
            'https://www.bloomberg.com/magazine/businessweek/20_32',
            'https://www.bloomberg.com/magazine/businessweek/20_31',
            #'https://www.bloomberg.com/magazine/businessweek/20_29',
            #'https://www.bloomberg.com/magazine/businessweek/20_28',
            #'https://www.bloomberg.com/magazine/businessweek/20_27',
            #'https://www.bloomberg.com/magazine/businessweek/20_26',
            #'https://www.bloomberg.com/magazine/businessweek/20_25',
            #'https://www.bloomberg.com/magazine/businessweek/20_23',
            # 'https://www.bloomberg.com/magazine/businessweek/20_22',
            # 'https://www.bloomberg.com/magazine/businessweek/20_21',
            # 'https://www.bloomberg.com/magazine/businessweek/20_20',
            # 'https://www.bloomberg.com/magazine/businessweek/20_19',
            # "https://www.bloomberg.com/magazine/businessweek/20_18",
            # "https://www.bloomberg.com/magazine/businessweek/20_17",
            # "https://www.bloomberg.com/magazine/businessweek/20_15",
            # "https://www.bloomberg.com/magazine/businessweek/20_14",
            # "https://www.bloomberg.com/magazine/businessweek/20_13"
        ]
        for index_url in index_urls:
            inssue_num = index_url.split('/')[-1]
            while True:
                time.sleep(random.randrange(1, 5))
                proxies = {
                    'http': 'http://127.0.0.1:24000',
                    'https': 'http://127.0.0.1:24000',
                }
                ua=get_ua()
                try:
                    headers = {
                        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                        'accept-encoding': 'gzip, deflate, br',
                        'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'cache-control': 'max-age=0',
                        'sec-fetch-dest': 'document',
                        'sec-fetch-mode': 'navigate',
                        'sec-fetch-site': 'same-origin',
                        'user-agent': ua,
                        'upgrade-insecure-requests': '1',
                        'sec-fetch-user': '?1',
                    }
                    response = requests.request("GET", index_url, verify = False, headers=headers,proxies=proxies,timeout=30)
                    myhtml=etree.HTML(response.text)
                    if myhtml.xpath('//section[@class="story-list-module"]'):
                        break
                    else:
                        continue
                except Exception as e:
                    print(e)
                    continue
            sections=myhtml.xpath('//section[@class="story-list-module"]')
            inssue_update=''.join(myhtml.xpath('//h1[@class="section-front-header-module__title"]/text()'))
            for se in range(len(sections)):
                h_tag=''.join(sections[se].xpath('./div/h3[@class="story-list-module__title"]/text()')).strip().replace('\n','')
                print("h_tag：{h_tag}".format(h_tag=h_tag))
                h_tag_contents=sections[se].xpath('./div/article[@class="story-list-story mod-story"]')
                for h_t_c in range(len(h_tag_contents)):
                    bloomberg_json = {}
                    bloomberg_json['inssue_tag'] = h_tag
                    bloomberg_json['inssue_update']=inssue_update
                    bloomberg_json['daily_or_week']='bloomberg_week'
                    print("inssue_num：{inssue_num}".format(inssue_num=inssue_num))
                    bloomberg_json['inssue_num'] = inssue_num
                    bloomberg_json['website']='bloomberg'
                    h_title=''.join(h_tag_contents[h_t_c].xpath('./div/div[@class="story-list-story__info__headline"]/a/text()'))
                    print("h_title：{h_title}".format(h_title=h_title))
                    bloomberg_json['title']=h_title
                    h_author=h_tag_contents[h_t_c].xpath('./div/span[@class="story-list-story__info__byline"]//text()')
                    print("h_author：{h_author}".format(h_author=h_author))
                    bloomberg_json['authors']=h_author
                    h_href=''.join(h_tag_contents[h_t_c].xpath('./div/div[@class="story-list-story__info__headline"]/a/@href'))
                    if not h_href.startswith('http'):
                        h_href='https://www.bloomberg.com'+str(h_href)
                    print("h_href：{h_href}".format(h_href=h_href))
                    bloomberg_json['url'] = h_href
                    url_encryption = self.get_md5(h_href)
                    print(url_encryption)
                    bloomberg_json['url_encryption'] = url_encryption
                    while True:
                        time.sleep(random.randrange(1, 5))
                        proxies = {
                            'http': 'http://127.0.0.1:24000',
                            'https': 'http://127.0.0.1:24000',
                        }
                        ua=get_ua()
                        print(ua)
                        try:
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
                            response = requests.request("GET", h_href, verify = False, headers=headers,proxies=proxies,timeout=30)
                            myhtml = etree.HTML(response.text.encode('utf-8'))
                            if myhtml.xpath('//script[@type="application/ld+json"]/text()')[0]:
                                break
                            else:
                                continue
                        except Exception as e:
                            print(e)
                            continue
                    head_data=myhtml.xpath('//script[@type="application/ld+json"]/text()')[0]
                    head_data_json=json.loads(head_data)
                    headline=head_data_json['headline']
                    h_content = myhtml.xpath('//div[@class="lede-vertical-image-text-right__dek"]/p//text()|//div[@class="lede-text-only__dek"]/span/p//text()|//div[@class="lede-text-only__dek"]/span//text()|//div[@class="abstract__item-text"]//text()|//div[@class="lede-text-only__conten"]/span[@class="lede-text-only__highlight"]//text()|//div[@class="full-width-image-lede-text-below__dek"]/p//text()|//div[@class="lede-text-v2__dek"]/p//text()|//div[@class="not-quite-full-width-image-lede-text-above__dek"]/p//text()|//div[@class="lede-text-only__dek"]/p//text()|//div[@class="lede-vertical-image-text-left__dek"]/text()|//div[@class="lede-text-v2__dek"]/text()|//div[@class="not-quite-full-width-image-lede-text-above__dek"]/text()|//div[@class="lede-vertical-image-text-left__dek"]/p//text()|//div[@class="sc-jqCOkK fWCLYP theme-bw Lede__Dek css--lede-dek"]//text()|//div[@class="sc-jqCOkK gwpjrP theme-bw Lede__Dek css--lede-dek"]/p/text()|//div[@class="sc-jqCOkK jHiAWd theme-bw Lede__Dek css--lede-dek"]/p/text()|//div[@class="sc-jbKcbu cGRTQk theme-bw Lede__Dek css--lede-dek"]/p/text()|//div[@class="sc-jbKcbu eqmHEo theme-bw Lede__Dek css--lede-dek"]/p/text()')
                    h_contents=[]
                    for h_c in h_content:
                        h_c=h_c.strip().replace('\n','').replace('\xa0','').replace('â','').replace('\x80','').replace('\x99','').replace('\x94','').replace('\x98','').replace('\x9c','')
                        if h_c:
                            h_contents.append(h_c)
                    if h_contents:
                        h_contents=h_contents
                    else:
                        h_contents=head_data_json['description']
                    print("h_contents：{h_contents}".format(h_contents=h_contents))
                    bloomberg_json['head_content']=h_contents
                    author = myhtml.xpath('//div[@class="author"]/text()|//div[@class="author-v2"]/a/text()')
                    authors=[]
                    for au in author:
                        au=au.strip().replace('\n','')
                        if au:
                            authors.append(au)
                    datePublished_t=head_data_json['datePublished']
                    print(datePublished_t)
                    GMT_FORMAT = '%a, %d %b %Y %H:%M:%S GMT'
                    try:
                        datePublished=datetime.strptime(datePublished_t, GMT_FORMAT)
                        bloomberg_json['datepublished'] = datePublished
                    except Exception as e:
                        datePublished=datePublished_t
                        bloomberg_json['datepublished'] = datePublished
                        print(e)
                    image_url = myhtml.xpath('//div[@class="lazy-img lede-vertical-image-text-right__image"]/img/@src|//div[@class="lazy-img "]/img/@src|//div[@class="video-player__curtain"]/img/@src|//div[@class="lazy-img full-width-image-lede-text-below__image"]/img/@src|//img[@class="lazy-img__image loaded"]/@src|//div[@class="lazy-img not-quite-full-width-image-lede-text-above__image"]/img/@src|//div[@class="lazy-img lede-vertical-image-text-left__image"]/img/@src|//figure[@class="sc-frDJqD krdlRH fullscreen_dek_below image__group__container__inner css--lede-image-inner-wrapper"]/img/@src|//figure/div[@class="lazy-img not-quite-full-width-image-lede-text-below__image"]/img/@src')
                    if image_url:
                        image_url_new=parse.urljoin(image_url[0],'800x-1.jpg')
                        res = re.match('.*/(.*?\.\w+).*?', image_url_new).group(1)
                        image_image=image_url_new.split('/')[-5]+'_'+image_url_new.split('/')[-4]+'_'+image_url_new.split('/')[-3]+'_' +image_url_new.split('/')[-2]+'_'+res
                        root = ".\\image\\"
                        path = root +image_image
                        if not os.path.exists(root):
                                os.mkdir(root)
                        if not os.path.exists(path):
                            with open(path, 'wb') as f:
                                while True:
                                    try:
                                        if requests.get(url=image_url_new, verify = False, proxies=proxies, timeout=30
                                                        ).content:
                                            f.write(requests.get(url=image_url_new, verify = False, proxies=proxies,
                                                                 timeout=30
                                                                 ).content)
                                            break
                                        else:
                                            continue
                                    except Exception as e:
                                        print(e)
                                        continue
                            f.close()
                        image_url='https://trends.lenovoresearch.cn/'+image_image
                        bloomberg_json['header_image_src']=image_url
                    contents=[]                    
                    content_childrens = myhtml.xpath('//div[@class="body-copy fence-body"]/child::*|//div[@class="body-copy-v2 fence-body"]/child::*|//section[@class="sc-hSdWYo grFxQy css--root-container"]/child::*|//section[@class="sc-VigVT jLTRS css--root-container"]/child::*|//section[@class="sc-VigVT cTDtZJ css--root-container"]/child::*')
                    if 'script' not in str(content_childrens):
                        for cc in range(len(content_childrens)):
                            if content_childrens[cc].xpath('./text()|./a/text()|./em/text()|./em/a/text()|./a/em/text()|./p/text()|./p/a/text()|./p/em/text()|./div/p/em/text()|./p/a/em/text()'):
                                cc_content=''.join(content_childrens[cc].xpath('./text()|./a/text()|./em/text()|./em/a/text()|./a/em/text()|./p/text()|./p/a/text()|./p/em/text()|./div/p/em/text()|./p/a/em/text()')).strip().replace('\n','').replace('()','').replace('\xa0','').replace('â','').replace('\x80','').replace('\x99','').replace('\x94','').replace('\x98','').replace('\x9c','')
                                if cc_content:
                                    contents.append(cc_content)
                                continue
                            elif content_childrens[cc].xpath('./div[@class="image"]/div[@class="lazy-img"]/img/@src|./div/div/figure/div/img/@src|./div/div/div/figure/div/img/@src|./div/figure/div/img/@src'):
                                cc_img_src=content_childrens[cc].xpath('./div[@class="image"]/div[@class="lazy-img"]/img/@src|./div/div/figure/div/img/@src|./div/div/div/figure/div/img/@src|./div/figure/div/img/@src')
                                if cc_img_src:
                                    if len(cc_img_src)==1:
                                        cc_img_src=cc_img_src[0]
                                        cc_img_src_new = parse.urljoin(cc_img_src, '800x-1.jpg')
                                        res = re.match('.*/(.*?\.\w+).*?', cc_img_src_new).group(1)
                                        image_image = cc_img_src_new.split('/')[-5] + '_' + cc_img_src_new.split('/')[
                                            -4] + '_' + cc_img_src_new.split('/')[-3] + '_' + cc_img_src_new.split('/')[
                                                        -2] + '_' + res
                                        root = ".\\image\\"
                                        path = root + image_image
                                        if not os.path.exists(root):
                                            os.mkdir(root)
                                        if not os.path.exists(path):
                                            with open(path, 'wb') as f:
                                                while True:
                                                    try:
                                                        if requests.get(url=cc_img_src_new,verify = False,proxies=proxies,timeout=30
                                                                    ).content:
                                                            f.write(requests.get(url=cc_img_src_new, verify = False,proxies=proxies, timeout=30
                                                                        ).content)
                                                            break
                                                        else:
                                                            continue
                                                    except Exception as e:
                                                        print(e)
                                                        continue
                                            f.close()
                                        cc_img_src = 'https://trends.lenovoresearch.cn/' + image_image
                                        contents.append(cc_img_src)
                                    elif len(cc_img_src)>1:
                                        for c_i_s in cc_img_src:
                                            c_i_s_new = parse.urljoin(c_i_s, '800x-1.jpg')
                                            res = re.match('.*/(.*?\.\w+).*?', c_i_s_new).group(1)
                                            image_image = c_i_s_new.split('/')[-5] + '_' + c_i_s_new.split('/')[
                                                -4] + '_' + c_i_s_new.split('/')[-3] + '_' + c_i_s_new.split('/')[
                                                            -2] + '_' + res
                                            root = ".\\image\\"
                                            path = root + image_image
                                            if not os.path.exists(root):
                                                os.mkdir(root)
                                            if not os.path.exists(path):
                                                with open(path, 'wb') as f:
                                                    while True:
                                                        try:
                                                            if requests.get(url=c_i_s_new, verify = False,proxies=proxies, timeout=30
                                                                            ).content:
                                                                f.write(requests.get(url=c_i_s_new,verify = False, proxies=proxies,
                                                                                    timeout=30
                                                                                    ).content)
                                                                break
                                                            else:
                                                                continue
                                                        except Exception as e:
                                                            print(e)
                                                            continue
                                                f.close()
                                            c_i_s = 'https://trends.lenovoresearch.cn/' + image_image
                                            contents.append(c_i_s)
                                continue
                            else:
                                continue
                    else:
                        soup = BeautifulSoup(response.text)
                        content_childrens = soup.select('div[class^="body-copy fence-body"],div[class^="body-copy-v2 fence-body"],section[class^="sc-hSdWYo grFxQy css--root-container"],section[class^="sc-VigVT jLTRS css--root-container"],section[class^="sc-VigVT cTDtZJ css--root-container"]')
                        contents = []
                        for group in content_childrens:
                            group = list(group)
                            for cc in range(len(group)):
                                try:
                                    if cc == 0:
                                        if group[cc].select('h1'):
                                            contents.append(group[cc].h1.get_text())
                                        if group[cc].select('div[class^="sc-ktHwxA bCgLjf theme-bw Lede__Dek css--lede-dek"]'):
                                            contents.append(getText(group[cc].select('div[class^="sc-ktHwxA bCgLjf theme-bw Lede__Dek css--lede-dek"] > p')))
                                    if cc > 0:
                                        contents.append(self.getText(group[cc].find_all(['p','img'])))
                                        
                                except Exception as e:
                                    continue
                    bloomberg_json['contents']=contents
                    print("contents：{contents}".format(contents=contents))
                    contents_search='###'.join(contents)
                    bloomberg_json['contents_search']=contents_search
                    try:
                        es_client.index(index="news2020", doc_type="_doc", id=url_encryption, body=bloomberg_json)
                        print("保存数据成功")
                    except Exception as e:
                        print(e)

if __name__ == '__main__':
    spider = news()
    spider.bloomberg_index()