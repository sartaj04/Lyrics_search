import scrapy
import csv

class SongItem(scrapy.Item):
    name = scrapy.Field()

class SongSpider(scrapy.Spider):
    name = "song"
    allowed_domains = ["genius.com"]
    start_urls = [
        'https://genius.com/artists-index/z',
    ]

    def parse(self, response):
        items = []
        for each in response.xpath('//ul[@class="artists_index_list"]'):

            item = SongItem()

            name = each.xpath("li/a/text()").extract()
            print(name)
            # xpath返回的是包含一个元素的列表
            for i in range(0, len(name)):
                item['name'] = name[i]
                item = SongItem()
                items.append(item)

            # 直接返回最后数据
        return items
        # 获取网站标题
        # context = response.xpath('//ul[@class="artists_index_list"]/li/a/text()')
        # SongItem
        # 提取网站标题
        # name = context.extract()
        # print(name)
        # pass

