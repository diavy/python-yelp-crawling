import scrapy
from doctor_review.items import DoctorReviewItem


# selected_cities = [
#     "Los Angeles, CA", "San Diego, CA", "San Jose, CA", "San Francisco, CA",
#     "New York, NY",  "Buffalo, NY", "Rochester, NY",
#     "Philadelphia, PA", "Pittsburgh, PA",
#     "Phoenix, AZ", "Tucson, AZ",
#     "Boston, MA",
#     "Baltimore, MD",
#     "Denver, CO", "Colorado Spring, CO",
#     "Hamilton, ON", "Toronto, ON", "Vancouver, BC", "Calgary, AB",
# ]

selected_cities = [
    "Hamilton, Ontario",  "Edmonton, Ontario",  "Toronto, Ontario", "Ottawa, Ontario",
    "Vancouver,  British Columbia",
    "Calgary, Alberta",
    "Montreal, Quebec",
]

#basic_url = "https://www.yelp.com/search?find_desc=knee+pain&find_loc={}&cflt=health,massage,chiropractors,acupuncture,physicaltherapy,physicians,massage_therapy,naturopathic,medicalspa,medcenters,hospitals,reflexology,reiki,sportsmed,active,fitness,tcm,osteopathicphysicians"
basic_url = "https://www.yelp.com/search?find_desc=knee+pain&find_loc={}&cflt=health,massage"


class YelpSpider(scrapy.Spider):
    name = "yelp"
    start_urls = [basic_url.format(x) for x in selected_cities]


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
        # item['city'] = response.xpath('//span[@itemprop="addressLocality"]/text()').extract()
        # item['state'] =response.xpath('//span[@itemprop="addressRegion"]/text()').extract()
        item['city'] = response.xpath('//span[@itemprop="addressLocality"]/text()').extract()
        item['state'] = response.xpath('//span[@itemprop="addressRegion"]/text()').extract()
        item['street'] = response.xpath('//span[@itemprop="streetAddress"]/text()').extract()
        item['zipcode'] = response.xpath('//span[@itemprop="postalCode"]/text()').extract()

        # for review in response.xpath('//div[@class="review-content"]'):
        #     item['review_stars'] = review.xpath('.//div/meta/@content').extract()
        #     item['review_content'] = review.xpath('.//p/text()').extract()
        #     yield item

        for review in response.xpath('//div[@class="review review--with-sidebar"]'):
            item['reviewer_name'] = review.xpath('.//li[@class="user-name"]/a[@class="user-display-name"]/text()').extract()
            item['reviewer_city'] = review.xpath('.//li[@class="user-location responsive-hidden-small"]/b/text()').extract()
            item['reviewer_friends_count'] = review.xpath('.//li[@class="friend-count responsive-small-display-inline-block"]/b/text()').extract()
            item['reviewer_reviews_count'] = review.xpath('.//li[@class="review-count responsive-small-display-inline-block"]/b/text()').extract()
            #
            item['review_date'] = review.xpath('.//span[@class="rating-qualifier"]/text()').extract()
            #item['review_stars'] = review.xpath('//div[@itemprop="reviewRating"]/meta[@itemprop="ratingValue"]/@content').extract()
            #item['review_stars'] = review.xpath('.//div[@class="rating-very-large"]/i/@title').extract()
            #item['review_stars'] = review.xpath('.//div/meta/@content').extract()
            item['review_stars'] = review.xpath('.//div[@class="biz-rating biz-rating-large clearfix"]/div/div/@title').extract()

            item['review_content'] = review.xpath('.//div[@class="review-content"]/p/text()').extract()

            yield item


        next_page = response.xpath('//div/a[@class="u-decoration-none next pagination-links_anchor"]/@href')
        if next_page:
            url = response.urljoin(next_page[0].extract())
            #print(url)
            yield scrapy.Request(url, self.parse_review)


