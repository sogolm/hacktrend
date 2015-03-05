
from HackerNewsUnofficial import getPostsForDate


def dateGenerator():
   '''Returns a list of dates in the format yyyymmdd from '2014122' up to '20141231'.'''

   lst = []
   day = 24
   month = 12
   year = 2014

   while year < 2015:
      while month < 13:
         while day <= 31:
            today = ''
            if day < 10 and month < 10:
               today += str(year)+'0'+str(month)+'0'+str(day)
            if day < 10 and month >= 10:
               today += str(year)+str(month)+'0'+str(day)
            if day >= 10 and month < 10:
               today += str(year)+'0'+str(month)+str(day)
            if day >= 10 and month >= 10:
               today += str(year)+str(month)+str(day)
            lst.append(today)
            day += 1
            if month == 2 and day == 29 and year != 2012:
               day = 1
               break
            if month == 2 and day == 30 and year == 2012:
               day = 1
               break
            if month in [4, 6, 9, 11] and day == 31:
               day = 1
               break
         day = 1
         month += 1
         if month == 13:
            month = 1
            break
      moth = 1
      day = 1
      year += 1
   return lst



if __name__ == "__main__":
    dates = dateGenerator()
    for date in dates:
        print "Retrieving date: {date}".format(date=date)
        getPostsForDate(date)