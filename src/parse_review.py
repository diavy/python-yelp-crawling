#!/usr/local/bin/python3.4
"""
Parse the crawled review
"""

# import standard module #
import os
import re
import json
import ast
# import third-party module #
import pandas as pd
import numpy as np
from alchemy_python.alchemyapi import AlchemyAPI
# import user defined module #

# define global directories #
DAT = "../data"
CRAWLED_REVIEWS = "../data/doctor_reviews.json"
CRAWLED_SFO_REVIEWS = "../data/sfo_reviews.json"
api_key_file = "alchemy_python/api_key_old2.txt"

# define common data/directories #
selected_cities = {
    "Los Angeles, CA", "San Diego, CA", "San Jose, CA", "San Francisco, CA",
    "New York, NY",  "Buffalo, NY", "Rochester, NY",
    "Philadelphia, PA", "Pittsburgh, PA",
    "Phoenix, AZ", "Tucson, AZ",
    "Boston, MA",
    "Baltimore, MD",
    "Denver, CO", "Colorado Spring, CO"
}

black_words = ['hair', 'salon', 'dentist', 'tattoo', 'wax', 'nail', 'skin', 'gym', 'dental',
               'optomet', 'optic', 'eye', 'oral']

white_categories = {"Health", "Massage", "Chiropractors", "Acupuncture", "Physical Therapy",
                    "Doctors", "Massage Therapy", "Naturopathic", "Holistic", "Medical Spas", "Medical Centers",
                    "Hospitals", "Reflexology", "Reiki", "Sports Medicine", "Active Life", "Fitness",
                    "Traditional Chinese Medicine", "Osteopathic Physicians",
                    }


def is_valid1(name, category, city, black_pat):
    """Judge whether the review is valid"""

    if black_pat.search(name):
        return False
    elif 'Gym' in category:
        return False

    #elif city not in selected_cities:
    #    return False

    else:
        return True


def is_valid(name, category, city, black_pat):
    """Judge whether the review is valid medical related review"""
    if black_pat.search(name):
        return False
    # categories = set(category.split(', '))
    # if len(categories & white_categories) > 0:
    #     return True
    else:
        return True


def preprocess_reviews(review_raw, review_file, filtered_file):
    """parse the crawled raw reviews into clean format for following analysis"""
    review_item = []
    review_invalid_item = []
    black_pat = re.compile('|'.join(black_words), re.I)
    with open(review_raw, 'rU') as infile:
        for line in infile:
            if line.startswith("[") or line.startswith("]"):
                continue
            else:
                content = line.rstrip(',\n')
                review = json.loads(content)

            name = review['name']
            category = ', '.join(review['category'])
            stars = review["review_stars"][0].split()[0]
            content = ' '.join(review['review_content']).replace(u"\u00A0", "").replace('\n', " ").replace("\r", " ")

            try:
                city = review['city'][0] + ', ' + review['state'][0]
            except:
                #print(name)
                #print(review['city'])
                #print(review['state'])
                #city = ''
                continue

            if is_valid(name, category, city, black_pat):
                review_item.append({'name':name, 'category':category, 'city':city, 'stars':stars, 'content':content})
            else:
                review_invalid_item.append({'name':name, 'category':category, 'city':city, 'stars':stars, 'content':content})


    review_item = pd.DataFrame(review_item)
    review_item.to_csv(review_file, index=False, columns=['name', 'category', 'city', 'stars', 'content'], sep='\t')
    #reviews = pd.read_csv(review_file, sep='\t')

    review_invalid_item = pd.DataFrame(review_invalid_item)
    review_invalid_item.to_csv(filtered_file, index=False, columns=['name', 'category', 'city', 'stars', 'content'], sep='\t')


def combine_files(file_list, combined_file):
    """Combine files with various types of symptoms into one"""
    reviews_list = [pd.read_csv(f, sep='\t') for f in file_list]
    #for file in file_list:
    #data = pd.read_csv(file, sep='\t')
    combined_reviews = pd.concat(reviews_list, ignore_index=True)
    combined_reviews.drop_duplicates(inplace=True)
    combined_reviews.sort_values(by='name', inplace=True)
    print(combined_reviews)
    combined_reviews.to_csv(combined_file, index=False)



def preprocess_reviews_with_address(review_raw, review_file, filtered_file):
    review_item = []
    review_invalid_item = []
    black_pat = re.compile('|'.join(black_words), re.I)
    with open(review_raw, 'rU') as infile:
        for line in infile:
            if line.startswith("[") or line.startswith("]"):
                continue
            else:
                content = line.rstrip(',\n')
                review = json.loads(content)

            name = review['name']
            category = ', '.join(review['category'])
            stars = review["review_stars"][0]
            content = ' '.join(review['review_content']).replace(u"\u00A0", "").replace('\n', " ").replace("\r", " ")

            try:
                zipcode = review['zipcode'][0]
                street = review['street'][0]
                city = review['city'][0] + ', ' + review['state'][0]
            except:
                #print(name)
                #print(review['city'])
                #print(review['state'])
                #city = ''
                continue

            if is_valid(name, category, city, black_pat):
                review_item.append({'name':name, 'category':category, 'city':city, 'stars':stars, 'content':content,
                                    'street':street, 'zipcode':zipcode})
            else:
                review_invalid_item.append({'name':name, 'category':category, 'city':city, 'stars':stars,
                                            'content':content, 'street':street, 'zipcode':zipcode})


    review_item = pd.DataFrame(review_item)
    review_item.to_csv(review_file, index=False, columns=['name', 'category', 'city', 'zipcode', 'street', 'stars', 'content'], sep='\t')
    #reviews = pd.read_csv(review_file, sep='\t')

    review_invalid_item = pd.DataFrame(review_invalid_item)
    review_invalid_item.to_csv(filtered_file, index=False, columns=['name', 'category', 'city', 'zipcode', 'street', 'stars', 'content'], sep='\t')


def count_business_locations(review_file, city_file):
    """Count the distribution of businesses in different cities"""
    reviews = pd.read_csv(review_file, sep='\t')
    #print(reviews)
    grouped = reviews.groupby(["city", "name"])
    city_count = {}
    for group_name, group in grouped:
        #print(name)
        #exit(1)'
        city = group_name[0]
        name = group_name[1]
        if city not in selected_cities:
            continue
        if city in city_count:
            city_count[city] += 1
        else:
            city_count[city] = 1

    city_count = pd.Series(city_count, name="count")
    city_count.sort_values(ascending=False, inplace=True)
    city_count.to_csv(city_file, index_label='city', index=True, sep='\t')


def stats_business_per_city(review_file, city_file):
    """Calculate the statistics of business per city"""
    reviews = pd.read_csv(review_file, sep='\t')
    #print(reviews)
    grouped = reviews.groupby(["city", "name"])

    city_info = []
    city_review_count = {}
    city_business_count = {}
    city_stars = {}

    for group_name, business in grouped:
        city = group_name[0]
        name = group_name[1]
        business_stars = business['stars'].mean()
        business_reviews_no = len(business['stars'])

        if city not in selected_cities:
            continue
        if city in city_review_count:
            city_review_count[city] += business_reviews_no
            city_business_count[city] += 1
            city_stars[city].append(business_stars)

        else:
            city_review_count[city] = business_reviews_no
            city_business_count[city] = 1
            city_stars[city] = [business_stars]


    for city in city_review_count.keys():
        stars = np.array(city_stars[city])
        city_info.append({"city":city, "review_count":city_review_count[city], "business_count":city_business_count[city],
            "stars_mean":stars.mean(), "stars_std":stars.std(), "stars_min":stars.min(), "stars_max":stars.max()})

    city_info = pd.DataFrame(city_info)
    city_info.to_csv(city_file, index=False, sep='\t', columns=["city", "business_count", "review_count",
                        "stars_mean", "stars_std", "stars_min", "stars_max"])


def retrieve_category_stars(review_file, cat_file):
    """Find all types of business entities and their associated stars"""
    reviews = pd.read_csv(review_file, sep='\t')
    grouped = reviews.groupby("name")

    cat_stars = {}
    cat_freq = {}
    cat_info = []
    for name, business in grouped:
        #print(name)
        category = business.reset_index().ix[0, "category"]
        #category = business['category']
        stars = business['stars'].mean()
        for cat in category.split(', '):
            cat = cat.strip()

            if cat in cat_freq:
                cat_freq[cat] += 1
            else:
                cat_freq[cat] = 1

            if cat in cat_stars:
                cat_stars[cat].append(stars)
            else:
                cat_stars[cat] = [stars]


    for cat, stars in cat_stars.items():
        stars = np.array(stars)
        cat_info.append({"category":cat, "freq":cat_freq[cat], "stars":stars.mean()})

    cat_stars = pd.DataFrame(cat_info)
    cat_stars.sort_values(by="freq", ascending=False, inplace=True)
    cat_stars.to_csv(cat_file, sep='\t', index=False)


def rank_business_from_stars(review_file, rating_file, segment=None):
    """Rank the business based on rating stars, note that one business may have different locations"""
    reviews = pd.read_csv(review_file, sep='\t')
    grouped = reviews.groupby(['name', 'street'])

    business_stars = []
    for group_name, group in grouped:
        name = group_name[0]
        street = group_name[1]
        zipcode = group.reset_index().ix[0, "zipcode"]
        stars = group['stars'].mean()
        count = len(group['stars'])

        business_stars.append({'name':name, 'street':street, 'zipcode':zipcode,
                               'stars':stars, 'review_count':count})

    business_stars = pd.DataFrame(business_stars)

    if segment:
        business_stars.sort_values(by=[segment, 'stars', 'review_count'], ascending=False, inplace=True)
        #stars_segment = business_stars.groupby(segment)
        #print(stars_segment)
    else:
        business_stars.sort_values(by=['stars', 'review_count'], ascending=False, inplace=True)

    print(business_stars)

    business_stars.to_csv(rating_file, columns=['name', 'street', 'zipcode', 'stars', 'review_count'], sep='\t', index=False)

def test_alchemyapi():
    """Test alchemyapi for text and image"""
    alchemyapi = AlchemyAPI(api_key_file)

    # first , text test #
    myText = "Yesterday dumb Bob destroyed my fancy iPhone in beautiful Denver, Colorado. I guess I will have to head over to the Apple Store and buy a new one."
    #response = alchemyapi.entities("text", myText)
    #print(response)
    response = alchemyapi.keywords("text", myText, {'sentiment': 1})

    #print ("Sentiment: ", response["docSentiment"]["type"])

    # then, image text #
    #myImage = 'http://pbs.twimg.com/profile_images/461288879610273792/0Ov0aVZB_normal.jpeg'
    #myImage = 'http://pbs.twimg.com/profile_images/378800000256690149/d589c341ec147c6c9254c8fb81049796_normal.png'
    #response = alchemyapi.faceTagging("url", myImage)
    print(response)


def analyze_zipcode_reviews(review_file, zipcode, parsed_file):
    """Anayze the sentiment of reviews from one zipcode"""
    reviews = pd.read_csv(review_file, sep='\t')
    grouped = reviews.groupby(['zipcode', 'name'])
    alchemyapi = AlchemyAPI(api_key_file)

    count = 0
    start_index = 28328
    exceed_limit = False
    parsed_reviews = []
    for group_name, group in grouped:
        if group_name[0] == zipcode:
            print(group_name)
            for index, row in group.iterrows():
                if start_index > 0 and index != start_index:
                    continue
                start_index = -1
                content = row['content']
                name = row['name']
                street = row['street']
                stars = row['stars']
                #print(content)
                response = alchemyapi.keywords("text", content, {'sentiment': 1})
                #print(response)
                ##### parse the response #########
                if 'statusInfo' in response and response['statusInfo'] == 'daily-transaction-limit-exceeded':
                    print('daily quote exceeded...')
                    print(index)
            #       exit(1)
                    exceed_limit = True
                    break
                try:
                    parsed_content = response['keywords']
                    parsed_reviews.append({"name":name, "zipcode":zipcode, "street":street, "stars":stars,
                                      "content":content, "parsed_content":parsed_content})
                    #print(parsed_content)
                except:
                    continue

                count += 1
        if exceed_limit:
            break

    parsed_reviews = pd.DataFrame(parsed_reviews)
    parsed_reviews.to_csv(parsed_file, index=False, columns=['name', 'zipcode', 'street', 'stars',
                                                             'content', 'parsed_content'], sep='\t')
    print(count)


def parse_sentiment_keywords(parsed_file, keyword_file):
    """Count the keywords for positive and negative sentiment"""
    parsed_content = pd.read_csv(parsed_file, sep='\t')
    word_sentiment = {}
    for index, row in parsed_content.iterrows():
        try:
            keywords = ast.literal_eval(row['parsed_content'])
        except:
            print(keywords)
            continue
        #exit(1)
        for word in keywords:
            text = word['text']
            try:
                sentiment = float(word['sentiment']['score'])
            except:
                sentiment = 0.0
            if text in word_sentiment:
                word_sentiment[text].append(sentiment)
            else:
                word_sentiment[text] = [sentiment]

    sentiment_info = []
    for word, sentiment in word_sentiment.items():
        freq = len(sentiment)
        #if freq == 1:
        #    continue
        pos_scores = []
        neg_scores = []
        for score in sentiment:
            if score > 0:
                pos_scores.append(score)
            elif score < 0:
                neg_scores.append(score)
        if len(pos_scores) > 0:
            pos_score = sum(pos_scores)/len(pos_scores)
        else:
            pos_score = 0

        if len(neg_scores) > 0:
            neg_score = sum(neg_scores)/len(neg_scores)
        else:
            neg_score = 0
        sentiment_info.append({'keyword':word, 'frequency':freq, 'pos_score':pos_score, 'neg_score':neg_score})

    sentiment_info = pd.DataFrame(sentiment_info)
    sentiment_info.sort_values(by='frequency', ascending=False, inplace=True)
    sentiment_info.to_csv(keyword_file, sep='\t', index=False, columns=['keyword', 'frequency', "pos_score", "neg_score"])



if __name__ == '__main__':
    # pain_name = "knp"
    # raw_file = os.path.join(DAT, "canada_" + pain_name + ".json")
    # review_file = os.path.join(DAT, "canada_" + pain_name + ".csv")
    # filtered_file = os.path.join(DAT, "filtered_canada_" + pain_name + ".csv")
    # preprocess_reviews(raw_file, review_file, filtered_file)

    review_files = [os.path.join(DAT, "canada_" + pain_name + ".csv") for pain_name in ['lbp', 'lrbp', 'nep', 'knp']]
    combined_file = os.path.join(DAT, "canada_pain_clinics.csv")
    combine_files(review_files, combined_file)

    exit(1)

    city_loc_file = os.path.join(DAT, 'city_distribution.csv')
    #count_business_locations(review_file, city_loc_file)
    city_stats_file = os.path.join(DAT, 'city_stats.csv')
    #stats_business_per_city(review_file, city_stats_file)
    category_file = os.path.join(DAT, "category_stats.csv")
    #retrieve_category_stars(review_file, category_file)

    review_file = os.path.join(DAT, 'sfo_reviews.csv')
    filtered_file = os.path.join(DAT, 'invalid_sfo_reviews.csv')
    rating_file = os.path.join(DAT, 'sfo_business_ratings_rank.csv')
    #preprocess_reviews_with_address(CRAWLED_SFO_REVIEWS, review_file, filtered_file)
    rating_file = os.path.join(DAT, 'sfo_business_ratings_rank_per_zipcode.csv')
    #rank_business_from_stars(review_file, rating_file, segment='zipcode')
    #test_alchemyapi()
    zipcode=94114
    parsed_file = os.path.join(DAT, str(zipcode)+'_parsed_reviews_all.csv')
    keyword_file = os.path.join(DAT, str(zipcode)+'_sentiment_keywords.csv')
    #analyze_zipcode_reviews(review_file, zipcode, parsed_file)
    parse_sentiment_keywords(parsed_file, keyword_file)


