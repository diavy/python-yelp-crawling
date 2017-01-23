# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class DoctorReviewItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    #### basic information about the business ####
    name = scrapy.Field()
    review_content = scrapy.Field()
    #location = scrapy.Field()
    review_stars = scrapy.Field()
    review_count= scrapy.Field()
    review_date = scrapy.Field()
    category = scrapy.Field()

    #### geo code information ####
    city = scrapy.Field()
    state = scrapy.Field()
    street = scrapy.Field()
    zipcode = scrapy.Field()


    #### reviewer information ####
    reviewer_name = scrapy.Field()
    reviewer_city = scrapy.Field()
    reviewer_friends_count = scrapy.Field()
    reviewer_reviews_count = scrapy.Field()

