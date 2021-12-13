import scrapy 
from scrapy.item import Field


class Document(scrapy.Item):
    url = Field(default=None)
    text = Field(default=None)
    page_index = Field(default=None)
    label = Field(default=None)
    sub_label = Field(default=None)
    date = Field(default=None)
    
