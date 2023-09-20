#!/usr/bin/env python
# coding: utf-8

# In[13]:


from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

class UniqueWordCount(MRJob):

    def mapper(self, _, line):
        words = WORD_RE.findall(line.lower())
        for word in words:
            yield word, 1

    def reducer(self, word, counts):
        yield word, sum(counts)

if __name__ == '__main__':
    UniqueWordCount.run()

