from mrjob.job import MRJob
import re

class NonStopWordCount(MRJob):

    def mapper(self, _, line):
        words = re.findall(r'\w+', line.lower())

        stopwords = {'the', 'and', 'of', 'a', 'to', 'in', 'is', 'it'}

        for word in words:
            if word not in stopwords:
                yield (word, 1)

    def reducer(self, word, counts):
        yield (word, sum(counts))

if __name__ == '__main__':
    NonStopWordCount.run()
