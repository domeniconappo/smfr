import os

f = open(os.path.join(os.path.dirname(__file__), 'data/not_reconciled_tweets.log'))
find = 'Tweet was not reconciled with any collection! '
lines = [l.strip(find) for l in f.readlines() if l.startswith(find)]
out = '# -*- coding: utf-8 -*-\n\ndata = [' + ',\n'.join(lines) + '\n]'

with open(os.path.join(os.path.dirname(__file__), 'tweets_data.py'), 'w') as fout:
    fout.write(out)
