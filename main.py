import sys
import pymorphy2
from datasketch import MinHash, MinHashLSH
from elasticsearch import Elasticsearch
import bs4
from bs4 import BeautifulSoup as soup
from urllib.request import urlopen
from datetime import date
import requests
import os
import json
import hashlib

os.environ['ES_URL'] = 'http://localhost:9200/'
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
def mapping(es_object, index_name):
    settings = {
        "settings": {
        },
        "mappings": {
            "post": {
                "properties": {
                    "text": {
                        "type": "string"
                    },
                    "date": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "header2": {
                        "type": "string"
                    },
                    "author": {
                        "type": "string"
                    },
                    "url": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "title": {
                        "type": "string"
                    }
                }
            }
        }
    }
    es_object.indices.create(index=index_name,ignore=400, body=settings)
    return 'Индексы созадны'
mapping(es, 'DOW')

class Kinoart_article(object):

    def __init__(self, header='', header2='', date='', author='', page_url='', text=''):
        self.header = header
        self.header2 = header2
        self.date = date
        self.author = author
        self.page_url = page_url
        self.text = text

    def __str__(self):
        result = f'Заголовок статьи: {self.header}\n'+f'\nАвтор статьи: {self.author}\n'+f'\nДата публикации статьи: {self.date}\n'+f'\nURL статьи: {self.page_url}\n'+f'\nПодзаголовок статьи: {self.header2}\n'+f'\nТекст статьи: {self.text}'

        return result

    def to_json(self):
        return json.dumps({"title": self.header, "header_2": self.header2, "text": self.text, "url": self.page_url, "author": self.author, "date": self.date})

    def text_article(self):
        return self.text

    def push_to_ES(self):
        data = self.to_json()
        my_id = str(hash(self.text))
        url_put = os.getenv('ES_URL')
        url_put += 'article/post/' + my_id + '?pretty'
        headers = {'Content-Type': 'application/json'}

        r_put = requests.post(url_put, data=data, headers=headers)

        return r_put.text

class Kinoart_crawler(object):

    def __init__(self, url='https://kinoart.ru'):

        self.url = url
        self.future_urls = []
        self.articles = []
    def get_news_urls(self):

        res = requests.get(self.url)
        if res.status_code == 200:

            uClient = urlopen(self.url)
            page_html = uClient.read()
            uClient.close()

            page_soup = soup(page_html, "html.parser")
            news_array = page_soup.findAll("ul", {"class": "_2Ydn3"})
            news = news_array[0].findAll('li')

            self.future_urls = []

            for new in news:
                self.future_urls.append(self.url + new.a['href'])

    def get_full_new(self, url):

        res = requests.get(url)
        if res.status_code == 200:

            uClient = urlopen(url)
            page_html = uClient.read()
            uClient.close()

            page_soup = soup(page_html, "html.parser")

            found_header = page_soup.findAll("h1", {"class": "_3QT9w"})[0].text.replace('\xa0', ' ')

            header2 = page_soup.findAll("div", {"class": "hJ94J"})[0].div.text

            date = page_soup.findAll("span", {"class": "_1nOmx"})[0].text

            author = page_soup.findAll("a", {"class": "_26ewB"})[0].text

            text_raw = page_soup.findAll("div", {"class": "_3cO8U"})[0]

            text = ''

            for paragraph in text_raw:
                text += paragraph.text + "\n"

            return Kinoart_article(header=found_header, header2=header2, date=date, author=author, page_url=url, text=text)

    def show_articles(self):

        for i in range(len(self.articles)):
            print(f'Статья {i + 1}')
            print(self.articles[i])

    def get_all(self):

        self.get_news_urls()

        for ref in self.future_urls:
            article = self.get_full_new(ref)
            self.articles.append(article)

        self.show_articles()

    def push_articles_to_ES(self):
        for article in self.articles:
            article.push_to_ES()

def third(sourse):
    stop_symbols = '.,!?:;-\n\r()«»'
    stop_words = (u'это', u'как', u'так',
                  u'и', u'в', u'над',
                  u'к', u'до', u'не',
                  u'на', u'но', u'за',
                  u'то', u'с', u'ли',
                  u'а', u'во', u'от',
                  u'со', u'для', u'о',
                  u'же', u'ну', u'вы',
                  u'бы', u'что', u'кто',
                  u'он', u'она')
    return ( [x for x in [y.strip(stop_symbols) for y in sourse.lower().split()] if x and (x not in stop_words)] )

def initial_form(list):
    new_list = []
    morph = pymorphy2.MorphAnalyzer()
    for word in list:
        new_list.append(morph.parse(word)[0].normal_form)
    return new_list
###########################################################################
robot = Kinoart_crawler()
robot.get_all()
robot.articles[0].push_to_ES()
robot.push_articles_to_ES()
###########################################################################
print('Команды работы со статьей\n'
      '1. Поиск статей по ключевым словам\n'
      '2. MinHash+LSH\n'
      'Любое другое число для пропуска\n'
      'Введите значение:')
parametr = int(input())
if parametr == 1:
    print('Введите слово:')
    word = input()
    text_for_comparsion = []
    clear_text_for_comprasion = []
    i = 0
    x = 0
    while i < 10:
        text_one_article = robot.articles[i].text_article()
        i = i + 1
        text_for_comparsion.append(text_one_article)

    for x in range(len(text_for_comparsion)):
        clear_text_for_comprasion.append(third(text_for_comparsion[x]))
    for x in range(len(clear_text_for_comprasion)):
        for wordd in clear_text_for_comprasion[x]:
            if wordd == word:
                print('Такое слово найдено в статье ', x + 1)
if parametr == 2:
    print('Выбор статьи, у которой будет производиться поиск ближайших соседей:')
    number_articles = int(input())
    text_for_comparsion = []
    clear_text_for_comprasion = []
    comparsion_two = MinHash()
    i = 0
    x = 0
    while i < 10:
        text_one_article = robot.articles[i].text_article()
        i = i + 1
        text_for_comparsion.append(text_one_article)

    for x in range(len(text_for_comparsion)):
        clear_text_for_comprasion.append(third(text_for_comparsion[x]))

    comparsion_one = MinHash()
    for z in clear_text_for_comprasion[number_articles - 1]:
        comparsion_one.update(z.encode('utf8'))

    comparsion_two = MinHash()
    counter = 0
    print(clear_text_for_comprasion)
    for t in range(len(clear_text_for_comprasion)):
        if counter == (number_articles - 1):
            counter = counter + 1
        else:
            for c in clear_text_for_comprasion[counter]:
                comparsion_two.update(c.encode('utf8'))
            counter = counter + 1
            lsh = MinHashLSH(threshold=0.09)
            lsh.insert(counter, comparsion_two)
            result = lsh.query(comparsion_one)
            print("Ближайшие соседи(если существуют) с коэф.Джакарда >0.09 для",number_articles, "статьи", result)
else:
    sys.exit()

