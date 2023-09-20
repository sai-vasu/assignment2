#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from mrjob.job import MRJob
import re

STOPWORDS = set(["the", "and", "of", "a", "to", "in", "is", "it"])
WORD_RE = re.compile(r"[\w']+")

class NonStopwordCount(MRJob):

    def mapper(self, _, line):
        words = WORD_RE.findall(line.lower())
        for word in words:
            if word not in STOPWORDS:
                yield word, 1

    def reducer(self, word, counts):
        yield word, sum(counts)

if __name__ == '__main__':
    NonStopwordCount.run()

