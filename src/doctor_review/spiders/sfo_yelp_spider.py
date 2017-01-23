import scrapy
from doctor_review.items import DoctorReviewItem



class YelpSpider(scrapy.Spider):
    name = "sfo_yelp"
    start_urls = [
        "https://www.yelp.com/search?find_desc=lower+back+pain&find_loc=San+Francisco,+CA&start=0&cflt=health,massage,physicians,chiropractors,massage_therapy,acupuncture,physicaltherapy,active,fitness,tcm,sportsmed"
    ]


    def parse(self, response):
        """Parse search results webpages with a list of businesses"""
        for href in response.xpath('//span[@class="indexed-biz-name"]/a/@href'):
            url = response.urljoin(href.extract())
                #print(url)
            yield scrapy.Request(url, callback=self.parse_review)

        next_page = response.xpath('//div/a[@class="u-decoration-none next pagination-links_anchor"]/@href')
        if next_page:
            url = response.urljoin(next_page[0].extract())
            #print(url)
            yield scrapy.Request(url, self.parse)


    def parse_review(self, response):
        """Parse each review of each business entity"""
        item = DoctorReviewItem()

        item['name'] = response.xpath('//div/h1[starts-with(@class, "biz-page-title embossed-text-white")]/text()').extract()[0].strip()
        item['category'] = response.xpath('//span[@class="category-str-list"]/a/text()').extract()
        item['city'] = response.xpath('//span[@itemprop="addressLocality"]/text()').extract()
        item['state'] = response.xpath('//span[@itemprop="addressRegion"]/text()').extract()
        item['street'] = response.xpath('//span[@itemprop="streetAddress"]/text()').extract()
        item['zipcode'] = response.xpath('//span[@itemprop="postalCode"]/text()').extract()

        for review in response.xpath('//div[@class="review-content"]'):
            item['review_stars'] = review.xpath('.//div/meta/@content').extract()
            item['review_content'] = review.xpath('.//p/text()').extract()
            yield item


        next_page = response.xpath('//div/a[@class="u-decoration-none next pagination-links_anchor"]/@href')
        if next_page:
            url = response.urljoin(next_page[0].extract())
            #print(url)
            yield scrapy.Request(url, self.parse_review)


