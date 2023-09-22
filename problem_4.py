from mrjob.job import MRJob
import re

class InvertIndex(MRJob):

    def mapper(self, _, line):
        match = re.match(r'^(.*?):', line)
        if match:
            document = match.group(1)
        else:
            document = None

        words = re.findall(r'\w+', line.lower())

        for word in words:
            if document:
                yield (word, document)

    def reducer(self, word, documents):
        yield (word, list(set(documents)))

if __name__ == '__main__':
    InvertIndex.run()
