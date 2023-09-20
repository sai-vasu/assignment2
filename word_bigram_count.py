# -*- coding: utf-8 -*-
"""word_bigram_count.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1bMf1hLWp46Dra7qgg4wlUEoM5vfbR9wS
"""

from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

class WordBigramCount(MRJob):

    def configure_args(self):
        super(WordBigramCount, self).configure_args()
        self.add_passthru_arg('--stopwords', type=str, default='')

    def mapper(self, _, line):
        stopwords = set(self.options.stopwords.split(','))
        words = WORD_RE.findall(line.lower())
        for i in range(len(words) - 1):
            if words[i] not in stopwords and words[i + 1] not in stopwords:
                bigram = f"{words[i]},{words[i + 1]}"
                yield bigram, 1

    def reducer(self, bigram, counts):
        yield bigram, sum(counts)

if __name__ == '__main__':
    WordBigramCount.run()