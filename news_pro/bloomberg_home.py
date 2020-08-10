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

    def liExpand(self,content_children):
        temp = []
        for s in content_children:
            temp.append(s.xpath("./text()|./span/text()|./span/strong/a/text()|./span/a/span/text()|./span/a/text()|./span/a/strong/text()|./a/text()|./p/text()|./p/a/text()|./li/text()"))
            if s.xpath('./ul'):
                temp.append(self.liExpand(s.xpath('./ul')))
        return temp

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

    def bloomberg_home_index(self):
        print('开始执行')
        url = "https://www.bloomberg.com/"
        hrefs=[]
        while True:
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
                  'referer': 'https://www.bloomberg.com/',
                  'sec-fetch-dest': 'document',
                  'sec-fetch-site': 'same-origin',
                  'upgrade-insecure-requests': '1',
                  'user-agent': ua,
                }
                response = requests.request("GET", url, verify = False, headers=headers,proxies=proxies, data = payload,timeout=30)
                myhtml=etree.HTML(response.text)
                if myhtml.xpath('//section[@id="single_story2"]|//section[@id="hfs"]|//section[@id="hub_single_story"]'):
                    break
                else:
                    continue
            except Exception as e:
                print(e)
                continue
        section_one_ids=['single_story2','hfs','hub_single_story',]
        for section_one_id in section_one_ids:
            section_one=myhtml.xpath('//section[@id="{section_one_id}"]/article'.format(section_one_id=section_one_id))
            for s_o in range(len(section_one)):
                title=section_one[s_o].xpath('./section/a//text()')
                href=section_one[s_o].xpath('./section/a/@href')
                if 'https://www.bloomberg.com' in href[0]:
                    href = href[0]
                else:
                    href = 'https://www.bloomberg.com' + href[0]
                hrefs.append(href)
                if section_one[s_o].xpath('./section/div/div'):
                    section_one_div=section_one[s_o].xpath('./section/div/div')
                    for s_o_d in range(len(section_one_div)):
                        title=section_one_div[s_o_d].xpath('./a/text()')
                        href=section_one_div[s_o_d].xpath('./a/@href')
                        if 'https://www.bloomberg.com' in href[0]:
                            href = href[0]
                        else:
                            href = 'https://www.bloomberg.com' + href[0]
                        hrefs.append(href)
        section_two_ids=['flex_story_package','hub_story_package3','hub_story_package_2','hub_story_package_1','hub_story_package','story_package_2','story_package_with_live_video','opinion_package','story_package_3','flex_story_package']
        for section_two_id in section_two_ids:
            section_two=myhtml.xpath('//section[@id="{section_two_id}"]/div/article'.format(section_two_id=section_two_id))
            for s_t in range(len(section_two)):
                title=section_two[s_t].xpath('./h3/a//text()')
                href=section_two[s_t].xpath('./h3/a/@href')
                if href:
                    if 'https://www.bloomberg.com' in href[0]:
                        href=href[0]
                    else:
                        href = 'https://www.bloomberg.com' + href[0]
                    hrefs.append(href)
        zones=['zones=3,5','zones=5,7','zones=7,9','zones=9,11']
        for zone in zones:
            zone_url = "https://www.bloomberg.com/lineup/api/lazy_load/premium?{zone}".format(zone=zone)
            while True:
                try:
                    proxies = {
                        'http': 'http://127.0.0.1:24001',
                        'https': 'http://127.0.0.1:24001',
                    }
                    ua = get_ua()
                    payload = {}
                    headers = {
                      'accept': '*/*',
                      'accept-encoding': 'gzip, deflate, br',
                      'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
                      'referer': 'https://www.bloomberg.com',
                      'sec-fetch-dest': 'empty',
                      'sec-fetch-mode': 'cors',
                      'sec-fetch-site': 'same-origin',
                      'user-agent': ua,
                    }
                    response = requests.request("GET", zone_url, verify = False, headers=headers,proxies=proxies, data = payload,timeout=30)
                    content_json=json.loads(response.text)
                    if content_json['snippet']['html']:
                        break
                    else:
                        continue
                except Exception as e:
                    print(e)
                    continue
            content_json_h=content_json['snippet']['html']
            zone_myhtml=etree.HTML(content_json_h)
            section_three_ids=['green_lede','green_rt_rail']
            for section_three_id in section_three_ids:
                title=zone_myhtml.xpath('//section[@id="{section_three_id}"]/article/section/a/text()'.format(section_three_id=section_three_id))
                href=zone_myhtml.xpath('//section[@id="{section_three_id}"]/article/section/a/@href'.format(section_three_id=section_three_id))
                if href:
                    if 'https://www.bloomberg.com' in href[0]:
                        href=href[0]
                    else:
                        href = 'https://www.bloomberg.com' + href[0]
                    hrefs.append(href)
            section_four=zone_myhtml.xpath('//section[@id="green_1"]/div/article')
            for s_f in range(len(section_four)):
                title=section_four[s_f].xpath('./h3/a//text()')
                href=section_four[s_f].xpath('./h3/a/@href')
                if 'https://www.bloomberg.com' in href[0]:
                    href = href[0]
                else:
                    href = 'https://www.bloomberg.com' + href[0]
                hrefs.append(href)
            if zone_myhtml.xpath('//section[@id="single_story_3"]/article/section/a//text()'):
                section_five_title=zone_myhtml.xpath('//section[@id="single_story_3"]/article/section/a//text()')
                section_five_href=zone_myhtml.xpath('//section[@id="single_story_3"]/article/section/a/@href')
                if 'https://www.bloomberg.com' in section_five_href[0]:
                    href = section_five_href[0]
                else:
                    href = 'https://www.bloomberg.com' + section_five_href[0]
                hrefs.append(href)
            section_six_ids=['businessweek_line_2','election_1','screentime','3_up_heads_and_deks','story_package_7']
            for section_six_id in section_six_ids:
                section_six=zone_myhtml.xpath('//section[@id="{section_six_id}"]/div/article'.format(section_six_id=section_six_id))
                for s_s in range(len(section_six)):
                    title=section_six[s_s].xpath('./h3/a//text()')
                    href=section_six[s_s].xpath('./h3/a/@href')
                    if 'https://www.bloomberg.com' in href[0]:
                        href=href[0]
                    else:
                        href = 'https://www.bloomberg.com' + href[0]
                    hrefs.append(href)
            section_seven_ids=['story_package_5','story_package_8','opinion_2','hyperdrive','crypto','quicktakes_four_up','prognosis','economics','wealth']
            for section_seven_id in section_seven_ids:
                section_seven=zone_myhtml.xpath('//section[@id="{section_seven_id}"]/div/article'.format(section_seven_id=section_seven_id))
                for s_s in range(len(section_seven)):
                    title=section_seven[s_s].xpath('./h3/a//text()')
                    href=section_seven[s_s].xpath('./h3/a/@href')
                    if 'https://www.bloomberg.com' in href[0]:
                        href=href[0]
                    else:
                        href = 'https://www.bloomberg.com' + href[0]
                    hrefs.append(href)
            section_eight_ids=['bottom','feature','feature_1']
            for section_eight_id in section_eight_ids:
                title=zone_myhtml.xpath('//section[@id="{section_eight_id}"]/article/section/a//text()'.format(section_eight_id=section_eight_id))
                href=zone_myhtml.xpath('//section[@id="{section_eight_id}"]/article/section/a/@href'.format(section_eight_id=section_eight_id))
                if href:
                    if 'https://www.bloomberg.com' in href[0]:
                        href=href[0]
                    else:
                        href = 'https://www.bloomberg.com' + href[0]
                    hrefs.append(href)
        story_url = "https://personalization.bloomberg.com/user/recommendations/mfcfu?country=US&region=US&timezoneOffset=-28800000&application=javelin&limit=6&algorithm=mfcfu&resourceTypes=Story%3BInteractive%3BQuicktake%3BFeature&inactivePeriod=2592000000&interactionsLimit=25&maxAge=604800000&filters.exclude.Ni=FIVEEU%3BEURFIVE%3BAPACFIVE%3BAPACEVE%3BEUREVE%3BFIVE%3BUSEVE&thumbnailRequired=true&rescorers=random%3Btpx%3BsimilarResources&decayCoefficient=1&maxAge.ni.USSTOCKS=43200000&MFSimilarityThreshold=0.6&rescorers.random.minimumScore=0.1&rescorers.similarResources.minimumScore=0.01&rescorers.similarResources.groupSimilarResources=false"
        while True:
            try:
                proxies = {
                    'http': 'http://127.0.0.1:24001',
                    'https': 'http://127.0.0.1:24001',
                }
                ua = get_ua()
                payload = {}
                headers = {
                  'accept': '*/*',
                  'accept-encoding': 'gzip, deflate, br',
                  'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
                  'origin': 'https://www.bloomberg.com',
                  'referer': 'https://www.bloomberg.com/',
                  'sec-fetch-dest': 'empty',
                  'sec-fetch-mode': 'cors',
                  'sec-fetch-site': 'same-site',
                  'user-agent': ua,
                }
                response = requests.request("GET", story_url, verify = False, headers=headers, proxies=proxies,data = payload,timeout=30)
                story_json=json.loads(response.text)
                if story_json[0]['headline']:
                    break
                else:
                    continue
            except Exception as e:
                print(e)
        story_json=json.loads(response.text)
        for s_j in range(len(story_json)):
            title=story_json[s_j]['headline']
            href=story_json[s_j]['url']
            if 'https://www.bloomberg.com' in href:
                href = href
            else:
                href = 'https://www.bloomberg.com' + href
            hrefs.append(href)
        return hrefs
    def bloomberg_home_tail(self,hrefs):
        for href in hrefs:
            if href == 'https://www.bloomberg.com/coronavirus?srnd=premium':
                continue
            elif href == 'https://www.bloomberg.com/prognosis-podcast?srnd=premium':
                continue
            elif href == 'https://www.bloomberg.com/news/videos/2019-12-23/what-to-expect-from-the-crypto-market-in-2020-video?srnd=premium':
                continue
            elif href=='https://www.bloomberg.com/coronavirus?srnd=premium-asia':
                continue
            else:
                print(href)
                bloomberg_home_json = {}
                bloomberg_home_json['inssue_tag']='home'
                bloomberg_home_json['website'] = 'bloomberg'
                bloomberg_home_json['daily_or_week'] = 'bloomberg_daily'
                bloomberg_home_json['url'] = href
                url_encryption = self.get_md5(href)
                bloomberg_home_json['url_encryption']=url_encryption
                dsl = {
                    "query": {
                        "match": {
                            "_id": url_encryption,
                        },
                    }
                }
                results = es_client.search(index='news2020', body=dsl)
                res = results['hits']['hits']
                if res:
                    continue
                else:
                    while True:
                        time.sleep(random.randrange(1, 3))
                        proxies = {
                            'http': 'http://127.0.0.1:24001',
                            'https': 'http://127.0.0.1:24001',
                        }
                        ua = get_ua()
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
                            response = requests.request("GET", href, verify = False, headers=headers, data=payload, proxies=proxies, timeout=30)
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
                    authors = []
                    if 'author' in head_data_json.keys():
                        if type(head_data_json['author']) == str:
                            authors.append(head_data_json['author'])
                        elif type(head_data_json['author']) == list:
                            for au in head_data_json['author']:
                                author = au['name']
                                authors.append(author)
                    if not authors:
                        if myhtml.xpath('//div[@class="sc-gGBfsJ kxWLNH theme-bw Lede__Byline css--lede-byline"]/p/text()'):
                            authors = myhtml.xpath(
                                '//div[@class="sc-gGBfsJ kxWLNH theme-bw Lede__Byline css--lede-byline"]/p/text()')
                    if len(authors) == 0:
                        head_data = myhtml.xpath('//script[@type="text/javascript"]/text()')[0].strip()
                        if 'author' in head_data:
                            authors.append(re.findall(r'"author":(.*)',head_data)[0].split(',')[0][1:-1])
                    bloomberg_home_json['authors'] = authors
                    if 'datePublished' in head_data_json.keys():
                        datePublished_t = head_data_json['datePublished']
                        GMT_FORMAT = '%a, %d %b %Y %H:%M:%S GMT'
                        try:
                            datePublished = datetime.strptime(datePublished_t, GMT_FORMAT)
                            bloomberg_home_json['datepublished'] = datePublished
                        except Exception as e:
                            datePublished = datePublished_t
                            bloomberg_home_json['datepublished'] = datePublished
                            print(e)
                        bloomberg_home_json['datepublished'] = datePublished
                    
                    h_title = ''.join(myhtml.xpath(
                        '//h1[@class="lede-text-v2__hed"]//text()|//h1[@class="lede-text-only__hed"]//text()|//h1[@class="not-quite-full-width-image-lede-text-above__hed"]//text()|//h1[@class="container-width article-title"]//text()|//h1[@class="sc-tilXH sc-ktHwxA OsHyT css--lede-hed"]//text()|//h1[@class="copy-width article-title"]//text()|//h1[@class="sc-ktHwxA cQyJDv css--lede-hed"]//text()|//h1[@class="full-width-image-lede-text-above__hed"]//text()|//h1[@class="full-width-image-lede-text-overlay__hed"]//text()|//div[@class="lede-newsletter__newsletter-headline"]/text()|//h1[@class="not-quite-full-width-image-lede-text-below__hed"]//text()|//h1[@class="full-width-image-lede-text-below__hed"]//text()|//h1[@class="sc-tilXH hCUUXw css--lede-hed"]//text()|//div[@class="storythread-content__headline"]/h1//text()|//h1[@class="lede-text-v2__hed"]//text()'))
                    if not h_title and 'headline' in head_data_json.keys():
                        h_title = head_data_json['headline']
                    bloomberg_home_json['title'] = h_title
                    print('h_title：{h_title}'.format(h_title=h_title))
                    h_count = myhtml.xpath(
                        '//div[@class="lede-text-only__dek"]/p//text()|//div[@class="not-quite-full-width-image-lede-text-above__dek"]//text()|//div[@class="sc-jqCOkK gwpjrP theme-bw Lede__Dek css--lede-dek"]/p//text()|//div[@class="full-width-image-lede-text-above__dek"]//text()|//div[@class="full-width-image-lede-text-overlay__dek"]/p//text()|//div[@class="sc-jbKcbu eqmHEo theme-news Lede__Dek css--lede-dek"]/p//text()|//div[@class="lede-text-v2__dek"]/p/text()')
                    if not h_count and 'description' in head_data_json.keys():
                        h_count = head_data_json['description']
                    bloomberg_home_json['head_content']=h_count
                    if myhtml.xpath('//div[@class="lede-large-image-v2__image"]/@style'):
                        h_i=myhtml.xpath('//div[@class="lede-large-image-v2__image"]/@style')[0]
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
                        bloomberg_home_json['header_image_src'] = image_url
                    hh_image = myhtml.xpath(
                        '//div[@class="lazy-img lede-vertical-image-text-right__image"]/img/@src|//div[@class="lazy-img"]/img/@src|//div[@class="lazy-img "]/img/@src|//div[@class="video-player__curtain"]/img/@src|//div[@class="lazy-img full-width-image-lede-text-below__image"]/img/@src|//img[@class="lazy-img__image loaded"]/@src|//div[@class="lazy-img not-quite-full-width-image-lede-text-above__image"]/img/@src|//div[@class="lazy-img lede-vertical-image-text-left__image"]/img/@src|//figure[@class="sc-frDJqD krdlRH fullscreen_dek_below image__group__container__inner css--lede-image-inner-wrapper"]/img/@src|//figure/div[@class="lazy-img full-width-image-lede-text-above__image"]/img/@src|//figure/div[@class="lazy-img full-width-image-lede-text-overlay__mobile-image"]/img/@src|//figure[@class="splash-img"]/img/@src|//figure[@class="lede-image-container"]/div[@class="lede-image"]/img/@src|//div[@class="lazy-img not-quite-full-width-image-lede-text-below__image"]/img/@src|//figure/div[@class="lazy-img full-width-image-lede-text-below__image"]/img/@src|//div[@class="full-width-image-lede-no-text"]/figure/div/img/@src|//section[@class="main-column-v2"]/figure/div/div/img/@src|//figure[@class="lede-small-image-v2 lede figure-expandable"]/div/div/img/@src')
                    if hh_image:
                        h_image=hh_image[0]
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
                        bloomberg_home_json['header_image_src'] = image_url
                    contents = []
                    if myhtml.xpath('//section[@class="container-width"]/div[@class="copy-width copy-block"]'):
                        section_container = myhtml.xpath(
                            '//section[@class="container-width"]/div[@class="copy-width copy-block"]')
                        for s_con in range(len(section_container)):
                            s_con_child = section_container[s_con].xpath('./child::*')
                            for s_con_c in range(len(s_con_child)):
                                if s_con_child[s_con_c].xpath('./text()|./a/text()|./em/text()'):
                                    s_con_c_content = ''.join(
                                        s_con_child[s_con_c].xpath('./text()|./a/text()|./em/text()'))
                                    contents.append(s_con_c_content)
                        if myhtml.xpath('//section[@class="footer copy-width"]/child::*'):
                            section_footer = myhtml.xpath('//section[@class="footer copy-width"]/child::*')
                            for s_foot in range(len(section_footer)):
                                if section_footer[s_foot].xpath('./text()|./a/text()|./em/text()'):
                                    section_footer_content = ''.join(
                                        section_footer[s_foot].xpath('./text()|./a/text()|./em/text()'))
                                    contents.append(section_footer_content)
                    if myhtml.xpath('//p[@class="copy-width"]//text()'):
                        contents.append(''.join(myhtml.xpath('//p[@class="copy-width"]//text()')))
                    if myhtml.xpath('//section[@class="sc-VigVT jLTRS css--root-container"]/child::*'):
                        content_children = myhtml.xpath(
                            '//section[@class="sc-VigVT jLTRS css--root-container"]/child::*')
                        for c_c in range(len(content_children)):
                            if content_children[c_c].xpath(
                                    './p/text()|./p/a/text()|./p/em/text()|./p/span/text()'):
                                content_text = ''.join(
                                    content_children[c_c].xpath(
                                        './p/text()|./p/a/text()|./p/em/text()|./p/span/text()')).strip().replace(
                                    '\n', '')
                                if content_text:
                                    contents.append(content_text)
                                continue
                            elif content_children[c_c].xpath('./div[@class="image"]/div[@class="lazy-img"]/img/@src'):
                                content_img = content_children[c_c].xpath(
                                    './div[@class="image"]/div[@class="lazy-img"]/img/@src')
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
                                                        if requests.get(url=cc_img_src_new,verify = False, proxies=proxies, timeout=30
                                                                        ).content:
                                                            f.write(requests.get(url=cc_img_src_new,verify = False, proxies=proxies,
                                                                                 timeout=30
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
                                                                f.write(requests.get(url=c_i_s_new, verify = False,proxies=proxies,
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
                    if myhtml.xpath(
                            '//div[@class="body-copy-v2 fence-body"]/child::*|//div[@class="body-copy fence-body"]/child::*|//section[@class="sc-hSdWYo grFxQy css--root-container"]/child::*'):
                        content_children = myhtml.xpath(
                            '//div[@class="body-copy-v2 fence-body"]/child::*|//div[@class="body-copy fence-body"]/child::*|//section[@class="sc-hSdWYo grFxQy css--root-container"]/child::*')
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
                                if content_text:
                                    if '[{"platform":"facebook","label":"Share this article on Facebook (opens in a new window)"' in content_text:
                                        continue
                                    contents.append(content_text)
                                continue
                            elif content_children[c_c].xpath('./div[@class="image"]/div[@class="lazy-img"]/img/@src'):
                                content_img = content_children[c_c].xpath(
                                    './div[@class="image"]/div[@class="lazy-img"]/img/@src')
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
                                                        if requests.get(url=cc_img_src_new, verify = False,proxies=proxies, timeout=30
                                                                        ).content:
                                                            f.write(requests.get(url=cc_img_src_new, verify = False,proxies=proxies,
                                                                                 timeout=30
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
                                                            if requests.get(url=c_i_s_new, verify = False,proxies=proxies, timeout=30
                                                                            ).content:
                                                                f.write(requests.get(url=c_i_s_new, verify = False,proxies=proxies,
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
                    bloomberg_home_json['contents'] = contents
                    bloomberg_home_json['contents_search'] = contents_search
                    print('contents：{contents}'.format(contents=contents))
                    try:
                        result=es_client.index(index="news2020", doc_type="_doc", id=url_encryption, body=bloomberg_home_json)
                        print("保存数据成功")
                    except Exception as e:
                        print(e)
if __name__ == '__main__':
    spider = news()
    hrefs=spider.bloomberg_home_index()
    spider.bloomberg_home_tail(hrefs)



