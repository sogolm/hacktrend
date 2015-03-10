__author__ = 'sogolmoshtaghi'

import re
from string import punctuation
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize


def clean_tokenize(content):
    """
    Returns the text from htmlDoc using boilerpipe and removes
    stopwords, symbols and punctuation
    """
    additional_stopwords = ['said', 'u', 'would', 'like', 'many', 'also', 'could',
                            'mr', 'ms', 'mrs', 'may', 'even', 'say', 'much',
                            'going', 'might', 'dont', 'go', 'another', 'around',
                            'says', 'editor', "''", "``"]
    all_stopwords = set(stopwords.words('english') +
                        additional_stopwords +
                        list(punctuation))

    # If content was passed into the function as a list of strings, grab the first element
    if type(content) == 'list':
        content = content[0]

    if len(content) > 0:
        content = content.lower()

        #removing links
        content = re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+',
                         '', content, flags=re.DOTALL)

        # removing emails
        email_regex = re.compile(("([a-z0-9!#$%&'*+\/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+\/=?^_`"
                                "{|}~-]+)*(@|\sat\s)(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(\.|"
                                 "\sdot\s))+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)"))
        content = re.sub(email_regex, '', content)

        # removing numbers and newlines from content
        content = re.sub("\d+", " ", content)

        # tokenize
        word_list = word_tokenize(content)
        filtered = [w.encode('ascii', 'ignore').lower().replace('\u2605','')
                    for w in word_list if w.encode('ascii', 'ignore').lower().replace('\u2605','').replace('"','')
                    not in all_stopwords]

        lmtzr = WordNetLemmatizer()
        words = [lmtzr.lemmatize(w) for w in filtered]
        punc_regex = "[\.\t\,\:;\(\)\.]"
        return words
    else:
        return []
