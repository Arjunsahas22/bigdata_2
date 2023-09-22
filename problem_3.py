from mrjob.job import MRJob
import re

class Bigramcount(MRJob):

    def mapper(self, _, line):
        words = re.findall(r'\w+', line.lower())

        for i in range(len(words)-1):
            bigram = f"{words[i]},{words[i+1]}"
            yield (bigram, 1)

    def reducer(self, bigram, counts):
        yield (bigram, sum(counts))

if __name__ == '__main__':
    Bigramcount.run()
