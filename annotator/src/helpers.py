import math
import re
import os
import logging
from subprocess import Popen, PIPE

import ujson
import geotext

CNN_MAX_SEQUENCE_LENGTH = 100
regexp = {
    'ampersand': re.compile(r'\s+&amp;?\s+'),
    'retweet': re.compile(r'^RT @\w+\s?:\s*'),
    'mention': re.compile(r'@[A-Za-z0-9_]+\b'),
    'time_a': re.compile(r"\b\d\d?:\d\d\s*[ap]\.?m\.?\b", flags=re.IGNORECASE),
    'time_b': re.compile(r"\b\d\d?\s*[ap]\.?m\.?\b", flags=re.IGNORECASE),
    'time_c': re.compile(r"\b\d\d?:\d\d\b", flags=re.IGNORECASE),
    'url': re.compile(r'\bhttps?:\S+', flags=re.IGNORECASE),
    'broken_url': re.compile(r'\s+https?$', flags=re.IGNORECASE),
    'nochars': re.compile(r'[^\w\d\s:\'",.\(\)#@\?!/’_]+'),
    'newlines': re.compile(r'\n'),
    'double_spaces': re.compile(r'\s{2,}'),
}
models_path = os.path.join(os.environ.get('MODELS_PATH', '/'), 'models')
current_models_mapping = os.path.join(models_path, 'current-model.json')
logger = logging.getLogger(__name__)


def replace_locations_loc_text(raw_text, locations):
    """Replace locations in text by "_loc_" (before normalization)"""
    places = geotext.GeoText(raw_text)
    places_list = places.cities + places.countries
    lowercase_text_without_locations = raw_text.lower()
    for location in places_list + locations:
        lowercase_text_without_locations = re.sub(r"\b" + location.lower() + r"\b", '_loc_',
                                                  lowercase_text_without_locations)
    return lowercase_text_without_locations


def compute_pow10_feature(number):
    """Given a positive number, expresses it as a power of 10, e.g.: 3421 -> 1000s"""
    return "%d" % math.pow(10, math.floor(math.log10(number))) if number > 0 else "0"


def compute_metadata_features(tweet):
    """Compute features from a tweet's metadata"""
    features = []

    # User
    followers_count = tweet['user']['followers_count']
    features.append("_user_followers_%ss" % compute_pow10_feature(followers_count))

    verified = tweet['user']['verified']
    if verified:
        features.append("_user_is_verified")

    url = tweet['user']['url']
    if url:
        features.append("_user_has_url")

    bio = tweet['user']['description']
    if bio:
        features.append("_user_has_bio")

    # Tweet is RT or quote
    # (note that the retweet_count of a tweet obtained via filter is always zero
    #  because we have collected it the moment it was posted, so we focus
    #  on the number of RTs/quotes of its original tweet)
    retweets_or_quotes = 0
    if 'retweeted_status' in tweet:
        retweets_or_quotes += tweet['retweeted_status']['retweet_count']

    if 'quoted_status' in tweet:
        retweets_or_quotes += tweet['quoted_status']['quote_count']

    if retweets_or_quotes > 0:
        features.append("_retweet_or_quote_count_%ss" % compute_pow10_feature(retweets_or_quotes))

    # Create as a dictionary and return
    metadata_as_features = dict([(feature, 1) for feature in features])
    return metadata_as_features


def tweet_normalization_aggressive(text):
    """Perform aggressive normalization of text"""

    # Ampersand
    text = regexp['ampersand'].sub(' and ', text)

    # Re-tweet marking
    text = regexp['retweet'].sub('_USER_', text)

    # User mentions
    text = regexp['mention'].sub('_USER_', text)

    # Time
    text = regexp['time_a'].sub('_TIME_', text)
    text = regexp['time_b'].sub('_TIME_', text)
    text = regexp['time_c'].sub('_TIME_', text)

    # URLs
    text = regexp['url'].sub('_URL_', text)

    # Broken URL at the end of a line
    text = regexp['broken_url'].sub('_URL_', text)

    # Non-alpha non-punctuation non-digit characters
    text = regexp['nochars'].sub('_URL_', text)

    # Newlines and double spaces
    text = regexp['newlines'].sub(' ', text)
    text = regexp['double_spaces'].sub(' ', text)

    return text


def create_text_for_cnn(tweet, locations):
    """Tokenization/string cleaning for conv neural network: removes all quote characters;
    it also append metadata features at the end of the tweet."""

    # Remove locations
    lowercase_text_without_locations = replace_locations_loc_text(tweet['text'], locations)

    string = tweet_normalization_aggressive(lowercase_text_without_locations)

    string = re.sub(r"\\", "", string)
    string = re.sub(r"\'", "", string)
    string = re.sub(r"\"", "", string)

    # Add metadata features
    metadata_as_dict = compute_metadata_features(tweet)

    string += " " + " ".join(sorted(list(metadata_as_dict.keys())))
    return string.strip().lower()


def rchop(str_, ending):
    if str_.endswith(ending):
        return str_[:-len(ending)]
    return str_


def models_by_language(path):
    res = {}
    if os.path.exists(path):
        with open(path) as f:
            res = ujson.load(f)
    return res.get('model-by-language', {})


def update_models():

    git_command = ['/usr/bin/git', 'pull', 'origin', 'master']
    repository = os.path.join(models_path, '../')

    git_query = Popen(git_command, cwd=repository, stdout=PIPE, stderr=PIPE)
    git_status, error = git_query.communicate()
    logger.info(git_status)
    logger.info(error)
    return models_by_language(current_models_mapping)


models = update_models()
