'''
This module drives the scraping
'''

from hckernews_scrape import getPostsForDate
from date_generator import generate_dates


if __name__ == "__main__":
    dates = generate_dates()
    for date in dates:
        print "Retrieving date: {date}".format(date=date)
        getPostsForDate(date)