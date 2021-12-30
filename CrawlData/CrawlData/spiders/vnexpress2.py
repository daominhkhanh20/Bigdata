from scrapy import Spider, Request
from ..items import Document
import re 
from kafka import KafkaProducer
import json 

def serializer(message):
    return json.dumps(message).encode('utf-8')



class VnExpress(Spider):
    name = "vnexpress2"
    start_url = "https://vietnamnet.vn/vn/"
    list_label = [
        'the-gioi',
        'giao-duc',
        'doi-song',
    ]

    producer = KafkaProducer(
        bootstrap_servers=['dmk:9092'],
        value_serializer=serializer
    )

    def start_requests(self):
        for label in self.list_label:
            url_temp = self.start_url + label +'/trang{}'
            yield Request(url=url_temp.format(1), callback=self.parse, meta={'current_url': url_temp, 'page_index': 1})

    def start_requests(self):
        for label in self.list_label:
            url_temp = self.start_url + label +'/trang{}'
            yield Request(url=url_temp.format(1), callback=self.parse, meta={'current_url': url_temp, 'page_index': 1})
        
    def parse(self, response):
        url_temp = response.meta.get('current_url')
        page_index = response.meta.get('page_index')

        list_urls = response.xpath('//*[@class="clearfix item"]/a/@href').getall()
        if len(list_urls) == 0:
            list_urls = response.xpath('//*[@class="box-subcate-style4 m-b-20 clearfix"]/a/@href').getall()
        
        if len(list_urls) != 0:
            for url in list_urls:
                yield response.follow(url=url, callback=self.parser_content, meta={'page_index': page_index})
            
            if page_index < 1500:
                yield response.follow(url=url_temp.format(page_index+1), callback=self.parse, meta={'current_url': url_temp, 'page_index': page_index+1})

    
    def parser_content(self, response):
        url = response.url
        page_index = response.meta.get('page_index', None)  
        label = response.xpath('//*[@class="top-cate-head-title"]/a/text()').get()
        sub_label = response.xpath('//*[@class="top-cate-head-subcate-child"]/a/@title').get()
        text = response.xpath('//*[@class="ArticleContent"]/p/text()').getall()
        date= response.xpath('//*[@class="time-zone"]/text()').get()
        
        data = {
            'url': url,
            'page_index':page_index,
            'label':label,
            'sub_label':sub_label,
            'text': text,
            'date':date
        }
        
        self.producer.send('SmallData', data)