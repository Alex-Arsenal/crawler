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

    def listCombine(self,contents,i):
        text = []
        for s in contents:
            if type(s[0]) == list:
                text.append(self.listCombine(s[0],i+1))
            else:
                if type(s) == list:
                    text.append("list_level"+str(i)+" "+"".join(s))
                else:
                    text.append("list_level"+str(i)+" "+s)
        return text

    def flat(self,nums):
        res = []
        for i in nums:
            if isinstance(i, list):
                res.extend(self.flat(i))
            else:
                res.append(i)
        return res
    
    #获取文章的链接
    def bloomberg_index(self,inssue_tag):
        print('开始执行')
        inde_url = "https://www.bloomberg.com/{inssue_tag}".format(inssue_tag=inssue_tag)
        #通过fake信息不断请求页面
        while True:
            print('index')
            time.sleep(random.randrange(1,3))
            try:
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
                  'referer': 'https://www.bloomberg.com/{inssue_tag}'.format(inssue_tag=inssue_tag),
                  'sec-fetch-dest': 'document',
                  'sec-fetch-mode': 'navigate',
                  'sec-fetch-site': 'same-origin',
                  'sec-fetch-user': '?1',
                  'upgrade-insecure-requests': '1',
                  'user-agent': ua,
                }
                response = requests.request("GET", inde_url, verify = False, headers=headers, data = payload,proxies=proxies,timeout=30)
                myhtml=etree.HTML(response.text)
                #识别页面内容是否正常显示
                if myhtml.xpath('//section[@id="hero_1"]'):
                    break
                else:
                    continue
            except Exception as e:
                print(e)
                continue
        hrefs=[]
        #获得首篇文章的子链接
        section_one_href=myhtml.xpath('//section[@id="hero_1"]/article/section/a/@href')
        #获取成功拼接成文章链接存入列表
        if section_one_href:
            section_one_href = 'https://www.bloomberg.com' + section_one_href[0]
            hrefs.append(section_one_href)
        #获取首篇文章的题目
        section_one_title=myhtml.xpath('//section[@id="hero_1"]/article/section/a/text()')
        #检查相关文章，并保存链接
        if myhtml.xpath('//section[@id="hero_1"]/article/section/div/div'):
            section_one=myhtml.xpath('//section[@id="hero_1"]/article/section/div/div')
            for s_o in range(len(section_one)):
                section_one_href=section_one[s_o].xpath('./a/@href')
                if section_one_href:
                    if 'https://www.bloomberg.com' in section_one_href[0]:
                        section_one_href=section_one_href[0]
                    else:
                        section_one_href='https://www.bloomberg.com'+section_one_href[0]
                    print(section_one_href)
                    hrefs.append(section_one_href)
                section_one_title = section_one[s_o].xpath('./a/text()')
        #所有子文章所在的section
        #第一个section模块
        section_hub_ids=['hub_story_list','story_list_4']
        for section_hub_id in section_hub_ids:
            if myhtml.xpath('//section[@id="{section_hub_id}"]/div/article'.format(section_hub_id=section_hub_id)):
                section_l=myhtml.xpath('//section[@id="{section_hub_id}"]/div/article'.format(section_hub_id=section_hub_id))
                for s_l in range(len(section_l)):
                    section_l_href=section_l[s_l].xpath('./div/div/a/@href')
                    if section_l_href:
                        if 'https://www.bloomberg.com' in section_l_href[0]:
                            section_l_href = section_l_href[0]
                        else:
                            section_l_href = 'https://www.bloomberg.com' + section_l_href[0]
                        hrefs.append(section_l_href)
                    section_l_title=section_l[s_l].xpath('./div/div/a/text()')
        tag_ids=['4_up_with_heds','4_up_heds_and_deks','4_up_with_images','4_up_images','3_up_with_images','_up_with_heds_1','g_1','apple','fang','amazon','netflix','google','crypto','hyperdrive']
        #第二个section模块
        for tag_id in tag_ids:
            if myhtml.xpath('//section[@id="{tag_id}"]/div/article'.format(tag_id=tag_id)):
                section_tag_id=myhtml.xpath('//section[@id="{tag_id}"]/div/article'.format(tag_id=tag_id))
                for s_t_i in range(len(section_tag_id)):
                    section_t_i_href=section_tag_id[s_t_i].xpath('./h3/a/@href')
                    if section_t_i_href:
                        if 'https://www.bloomberg.com' in section_t_i_href[0]:
                            section_t_i_href=section_t_i_href[0]
                        else:
                            section_t_i_href='https://www.bloomberg.com'+section_t_i_href[0]
                        hrefs.append(section_t_i_href)
                    section_t_i_title=section_tag_id[s_t_i].xpath('./h3/a/text()')
            else:
                continue
        #从源代码页面获取信息     
        index_url_h = "https://www.bloomberg.com/lineup/api/lazy_load/{inssue_tag}-vp?zones=3,5".format(inssue_tag=inssue_tag)
        while True:
            time.sleep(random.randrange(1,3))
            try:
                proxies = {
                    'http': 'http://127.0.0.1:24001',
                    'https': 'http://127.0.0.1:24001',
                }
                ua=get_ua()
                payload = {}
                headers = {
                  'accept-encoding': 'gzip, deflate, br',
                  'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
                  'referer': 'https://www.bloomberg.com/{inssue_tag}'.format(inssue_tag=inssue_tag),
                  'sec-fetch-dest': 'empty',
                  'sec-fetch-mode': 'cors',
                  'sec-fetch-site': 'same-origin',
                  'user-agent': ua,
                }
                response = requests.request("GET", index_url_h, verify = False, headers=headers, data = payload,proxies=proxies,timeout=30)
                content_json=json.loads(response.text)
                if content_json['snippet']['html']:
                    break
                else:
                    continue
            except Exception as e:
                print(e)
                continue
        content_json_h=content_json['snippet']['html']
        myhtml=etree.HTML(content_json_h)
        hub_section_ids=['crypto','3_up_heds_and_deks']
        for hub_section_id in hub_section_ids:
            if myhtml.xpath('//section[@id="{hub_section_id}"]/div/article'.format(hub_section_id=hub_section_id)):
                section_five=myhtml.xpath('//section[@id="{hub_section_id}"]/div/article'.format(hub_section_id=hub_section_id))
                for s_fi in range(len(section_five)):
                    section_five_href=section_five[s_fi].xpath('./h3/a/@href')
                    if section_five_href:
                        if 'https://www.bloomberg.com' in section_five_href[0]:
                            section_five_href=section_five_href[0]
                        else:
                            section_five_href='https://www.bloomberg.com'+section_five_href[0]
                        hrefs.append(section_five_href)
                    section_five_title=section_five[s_fi].xpath('./h3/a/text()')
        hub_ids=['hub_story_list_1','hub_story_list']
        for hub_id in hub_ids:
            if myhtml.xpath('//section[@id="{hub_id}"]/div/article'.format(hub_id=hub_id)):
                section_six=myhtml.xpath('//section[@id="{hub_id}"]/div/article'.format(hub_id=hub_id))
                for s_i in range(len(section_six)):
                    section_six_href=section_six[s_i].xpath('./div/div/a/@href')
                    if section_six_href:
                        if 'https://www.bloomberg.com' in section_six_href[0]:
                            section_six_href=section_six_href[0]
                        else:
                            section_six_href='https://www.bloomberg.com'+section_six_href[0]
                        hrefs.append(section_six_href)
                    section_six_title=section_six[s_i].xpath('./div/div/a/text()')
        return hrefs

    def liExpand(self,content_children):
        temp = []
        for s in content_children:
            temp.append(s.xpath("./text()|./span/text()|./span/strong/a/text()|./span/a/span/text()|./span/a/text()|./span/a/strong/text()|./a/text()|./p/text()|./p/a/text()|./li/text()"))
            if s.xpath('./ul'):
                temp.append(self.liExpand(s.xpath('./ul')))
        return temp

    #获取文章具体信息
    def bloomberg_tail(self, hrefs, inssue_tag):
        for href in hrefs:
            bloomberg_new_json = {}
            bloomberg_new_json['inssue_tag'] = inssue_tag
            bloomberg_new_json['website'] = 'bloomberg'
            bloomberg_new_json['daily_or_week'] = 'bloomberg_daily'
            bloomberg_new_json['url'] = href
            #加密过得url作为文章id存入数据库，检索数据库中是否有同样id的文章，有则跳过获取，没有则重新爬取
            url_encryption = self.get_md5(href)
            dsl = {
                    "query": {
                        "match": {
                            "_id":url_encryption,
                        },
                    }
                }
            results = es_client.search(index='news2020', body=dsl)
            res=results['hits']['hits']
            if res:
                continue
            else:
                bloomberg_new_json['url_encryption'] = url_encryption
                while True:
                    time.sleep(random.randrange(1, 3))
                    proxies = {
                        'http': 'http://127.0.0.1:24001',
                        'https': 'http://127.0.0.1:24001',
                    }
                    ua=get_ua()
                    try:
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
                        response = requests.request("GET", href, headers=headers, verify = False, data=payload, proxies=proxies, timeout=30)
                        myhtml = etree.HTML(response.text)
                        if myhtml.xpath('//script[@type="application/ld+json"]/text()')[0]:
                            break
                        else:
                            continue
                    except Exception as e:
                        print(e)
                        continue
                #获取文章的整体信息——作者，题目，图片地址，时间
                head_data = myhtml.xpath('//script[@type="application/ld+json"]/text()')[0]
                head_data_json = json.loads(head_data)
                authors = []
                if type(head_data_json['author'])==str:
                    author=head_data_json['author']
                    authors.append(author)
                elif type(head_data_json['author'])==list:
                    for au in head_data_json['author']:
                        author = au['name']
                        authors.append(author)
                if len(authors) == 0:
                    head_data = myhtml.xpath('//script[@type="text/javascript"]/text()')[0].strip()
                    authors.append(re.findall(r'"author":(.*)',head_data)[0].split(',')[0][1:-1])

                bloomberg_new_json['authors'] = authors
                datePublished_t = head_data_json['datePublished']
                GMT_FORMAT = '%a, %d %b %Y %H:%M:%S GMT'
                try:
                    datePublished = datetime.strptime(datePublished_t, GMT_FORMAT)
                    bloomberg_new_json['datepublished'] = datePublished
                except Exception as e:
                    datePublished = datePublished_t
                    bloomberg_new_json['datepublished'] = datePublished
                    print(e)
                bloomberg_new_json['datepublished'] = datePublished
                #两种方式获取题目
                #改版h_title=''.join(myhtml.xpath('//h1[@class="lede-text-v2__hed"]//text()|//div[@class="lede-newsletter__newsletter-headline"]//text()|//h1[@class="lede-text-only__hed"]/span//text()|//h1[@class="lede-text-only__hed"]//text()|//h1[@class="sc-tilXH sc-ktHwxA OsHyT css--lede-hed"]//text()|//h1[@class="copy-width article-title"]//text()'))
                h_title=''.join(myhtml.xpath('//h1[@class="lede-text-v2__hed"]//text()|//h1[@class="lede-text-only__hed"]/span//text()|//h1[@class="lede-text-only__hed"]//text()|//h1[@class="sc-tilXH sc-ktHwxA OsHyT css--lede-hed"]//text()|//h1[@class="copy-width article-title"]//text()'))
                if not h_title:
                    h_title = head_data_json['headline']
                bloomberg_new_json['title'] = h_title
                print('h_title：{h_title}'.format(h_title=h_title))
                
                if myhtml.xpath(
                        '//div[@class="lede-text-only__dek"]/p//text()|//div[@class="not-quite-full-width-image-lede-text-above__dek"]//text()|//div[@class="sc-jqCOkK gwpjrP theme-bw Lede__Dek css--lede-dek"]/p//text()|//div[@class="full-width-image-lede-text-above__dek"]//text()|//div[@class="full-width-image-lede-text-overlay__dek"]/p//text()'):
                    h_count = myhtml.xpath(
                        '//div[@class="lede-text-only__dek"]/p//text()|//div[@class="not-quite-full-width-image-lede-text-above__dek"]//text()|//div[@class="sc-jqCOkK gwpjrP theme-bw Lede__Dek css--lede-dek"]/p//text()|//div[@class="full-width-image-lede-text-above__dek"]//text()|//div[@class="full-width-image-lede-text-overlay__dek"]/p//text()')
                    bloomberg_new_json['head_content'] = h_count
                elif 'description' in head_data_json.keys():
                    h_count = head_data_json['description']

                #抓取文章图片
                if myhtml.xpath('//div[@class="lede-large-image-v2__image"]/@style'):
                    h_i = myhtml.xpath('//div[@class="lede-large-image-v2__image"]/@style')[0]
                    h_image = re.findall(u"'(.+)'", h_i)[0]
                    image_url_new = parse.urljoin(h_image, '800x-1.jpg')
                    res = re.match('.*/(.*?\.\w+).*?', image_url_new).group(1)
                    image_image = image_url_new.split('/')[-5] + '_' + image_url_new.split('/')[-4] + '_' + \
                                  image_url_new.split('/')[
                                      -3] + '_' + image_url_new.split('/')[-2] + '_' + res
                    root = ".\\image\\"
                    path = root + image_image
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
                    image_url = 'https://trends.lenovoresearch.cn/' + image_image
                    bloomberg_new_json['header_image_src'] = image_url

                hh_image = myhtml.xpath(
                    '//div[@class="lazy-img lede-vertical-image-text-right__image"]/img/@src|//div[@class="lazy-img"]/img/@src|//div[@class="lazy-img "]/img/@src|//div[@class="video-player__curtain"]/img/@src|//div[@class="lazy-img full-width-image-lede-text-below__image"]/img/@src|//img[@class="lazy-img__image loaded"]/@src|//div[@class="lazy-img not-quite-full-width-image-lede-text-above__image"]/img/@src|//div[@class="lazy-img lede-vertical-image-text-left__image"]/img/@src|//figure[@class="sc-frDJqD krdlRH fullscreen_dek_below image__group__container__inner css--lede-image-inner-wrapper"]/img/@src|//figure/div[@class="lazy-img full-width-image-lede-text-above__image"]/img/@src|//figure/div[@class="lazy-img full-width-image-lede-text-overlay__mobile-image"]/img/@src|//figure[@class="splash-img"]/img/@src|//figure[@class="sc-ksYbfQ MkQfN graphics_strip image__group__container__inner css--lede-image-inner-wrapper"]/img/@src')
                if hh_image:
                    h_image = hh_image[0]
                    image_url_new = parse.urljoin(h_image, '800x-1.jpg')
                    res = re.match('.*/(.*?\.\w+).*?', image_url_new).group(1)
                    image_image = image_url_new.split('/')[-5] + '_' + image_url_new.split('/')[-4] + '_' + \
                                  image_url_new.split('/')[
                                      -3] + '_' + image_url_new.split('/')[-2] + '_' + res
                    root = ".\\image\\"
                    #下载储存路径
                    path = root + image_image
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
                    image_url = 'https://trends.lenovoresearch.cn/' + image_image
                    bloomberg_new_json['header_image_src'] = image_url

                #获取段落信息及顺序
                content_children = myhtml.xpath('//div[@class="body-copy-v2 fence-body"]/child::*|//div[@class="body-copy fence-body"]/child::*|//section[@class="sc-VigVT lflzEu css--root-container"]/child::*|//div[@class="copy-block"]/child::*')
                contents = []
                for c_c in range(len(content_children)):
                    count = 0
                    if len(content_children[c_c]) > 0:
                        for s in content_children[c_c]:
                            if ' em ' in str(s):
                                count += 1
                        if count == len(content_children[c_c]):
                            continue
                    if content_children[c_c].xpath('./text()|./em/text()|./span/text()|./p/text()|./p/a/text()|./p/em/text()|./li/text()') and content_children[c_c].xpath('./text()|./em/text()|./span/text()|./p/text()|./p/a/text()|./p/em/text()|./li/text()')[0] != 'Read more: ':
                        temp = content_children[c_c].xpath('./text()|./a/text()|./strong/text()|./em/text()|./em/a/text()|./span/text()|./span/strong/text()|./p/text()|./p/strong/text()|./p/a/text()|./p/em/text()|./br')
                        for s in range(len(temp)):
                            if ' br ' in str(temp[s]):
                                temp[s] = '\n'    
                            if str(temp[s]).startswith("Read more:") or str(temp[s]).startswith("Get more") or str(temp[s]).startswith("Read next:") or temp[s] in content_children[c_c].xpath('./em/a/text()|./p/a/text()'):
                                temp[s] = ""    
                        content_text = ''.join(temp).strip()
                        
                        if (content_children[c_c].xpath('./li')):
                            #contents.append(str(self.liExpand(content_children[c_c])))
                            li_text = self.liExpand(content_children[c_c])
                            text = self.listCombine(li_text,1)                                    
                            contents.extend(self.flat(text))

                                
                        if content_text and content_text != "Read more:":
                            contents.append(content_text)
                        continue
                    elif content_children[c_c].xpath('./div[@class="image"]/div[@class="lazy-img"]/img/@src|./div/div/figure/div/img/@src'):
                        content_img = content_children[c_c].xpath('./div[@class="image"]/div[@class="lazy-img"]/img/@src|./div/div/figure/div/img/@src')
                        print('content_img：{content_img}'.format(content_img=content_img))
                        if content_img:
                            if len(content_img) == 1:
                                content_img = content_img[0]
                                cc_img_src_new = parse.urljoin(content_img, '800x-1.jpg')
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
                                                if requests.get(url=cc_img_src_new, verify = False, proxies=proxies, timeout=30
                                                                ).content:
                                                    f.write(requests.get(url=cc_img_src_new, verify = False, proxies=proxies, timeout=30
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
                            elif len(content_img) > 1:
                                for c_i in content_img:
                                    c_i_s_new = parse.urljoin(c_i, '800x-1.jpg')
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
                                                    if requests.get(url=c_i_s_new,verify = False, proxies=proxies, timeout=30
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
                contents_search = '###'.join(contents)
                bloomberg_new_json['contents'] = contents
                bloomberg_new_json['contents_search'] = contents_search
                print('contents：{contents}'.format(contents=contents))
                try:
                    es_client.index(index="news2020", doc_type="_doc", id=url_encryption, body=bloomberg_new_json)
                    print("保存数据成功")
                except Exception as e:
                    print(e)


if __name__ == '__main__':
    spider = news()
    inssue_tags=['markets','technology']
    for inssue_tag in inssue_tags:
        hrefs=spider.bloomberg_index(inssue_tag)
        spider.bloomberg_tail(hrefs,inssue_tag)

