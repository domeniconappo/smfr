import re

import langdetect


NO_LANGUAGE = 'no_language'
RECOGNIZED_LANGUAGES = ('es', 'en', 'fr', 'de', 'it')

# regexp
regexp = {'ampersand': re.compile(r'\s+&amp;?\s+'),
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


def safe_langdetect(text):
    """ Detects language of a lower case text """
    sanitized = tweet_normalization_aggressive(text)
    if len(sanitized) == 0:
        return NO_LANGUAGE
    else:
        try:
            return langdetect.detect(sanitized.lower())
        except langdetect.lang_detect_exception.LangDetectException:
            return NO_LANGUAGE
