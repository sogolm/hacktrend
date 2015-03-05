import requests
import grequests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from pymongo import Connection
import re


def getPostsForDate(date):
    """Returns a set of Posts for a YYYYMMDD date, using the hckernews.com API."""
    assert isinstance(date, str)

    # Retrieve the set of posts for the given date from the API.
    url = "http://hckrnews.com/data/{date}.js".format(date=date)
    res = requests.get(url, timeout=2)
    assert isinstance(res, requests.Response)
    if res.status_code is not 200:
        return []

    # Retrieve the JSON list of posts out of the response.
    jsonPosts = res.json()
    assert isinstance(jsonPosts, list)
    jsonPosts = filter(WhiteList.verifyPostJson, jsonPosts)

    # Resolve the linked content for each of the posts.
    urls = map(lambda json: json["link"], jsonPosts)
    reqs = (grequests.get(u, verify=False, timeout=) for u in urls)
    resps = grequests.map(reqs)
    assert (len(urls) == len(resps))

    map(lambda pair: setPostLinkedContent(pair[0], pair[1]), zip(resps, jsonPosts))

    conn = MongoClient()
    db = conn.hacker
    coll = db.posts
    coll.insert(jsonPosts)
    return coll



def setPostLinkedContent(response, post):
    """Extracts and associates the linked content with the post."""
    if response is None:
        return

    assert isinstance(response, requests.Response)
    assert isinstance(post, dict)

    # For unsuccessful responses, do nothing.
    post[u'status_code'] = response.status_code
        

    try:
        utf8PostContent = unicode(response.content, "utf-8")
        soup = BeautifulSoup(utf8PostContent)
    except:
        utf8PostContent = 'error'
    if utf8PostContent is not 'error':
        soup = BeautifulSoup(utf8PostContent)
        utf8PostText = soup.get_text()
        post[u"link_content"] = utf8PostText
        print "Retrieved data"

    else:
        post[u'link_content'] = 'unicode error'
        print 'encoding error'


def grequest_get(url):
    return grequests.get(url)


def getContentForUrl(url):
    """Gets the associated content to the link for testing purposes"""
    url_content = {}
    res = requests.get(url)
    if res.status_code is not 200:
        print "STATUS CODE: ", res.status_code
        return
    utf8PostContent = unicode(response.content, "utf-8")
    soup = BeautifulSoup(utf8PostContent)
    utf8PostText = soup.get_text()
    url_content[link] = utf8PostText


class WhiteList:
    @staticmethod
    def verifyPostJson(json):
        assert isinstance(json, dict)

        # Ignore posts with no score.
        if not u"points" in json or json[u"points"] is None or json[u"points"] is 0:
            return False

        link = json[u"link"]
        # Ignore pdf links, as they're too difficult to download reliably.
        if link.endswith(u"pdf"):
            return False

        return True