from scrapy import Spider, Request
from ..items import Document
import re 


class VnExpress(Spider):
    name = "vnexpress1"
    start_url = "https://vietnamnet.vn/vn/"
    list_label = [
        'thoi-su',
        'kinh-doanh',
        'giai-tri',
    ]

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
        item = Document()
        item['url'] = response.url
        item['page_index'] = response.meta.get('page_index', None)  
        item['label'] = response.xpath('//*[@class="top-cate-head-title"]/a/text()').get()
        item['sub_label'] = response.xpath('//*[@class="top-cate-head-subcate-child"]/a/@title').get()
        item['text'] = response.xpath('//*[@class="ArticleContent"]/p/text()').getall()
        item['date'] = response.xpath('//*[@class="time-zone"]/text()').get()
        
        yield item 
