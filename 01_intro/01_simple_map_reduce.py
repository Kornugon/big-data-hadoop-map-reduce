from mrjob.job import MRJob


# terminal:
# python 01_simple_map_reduce.py data.txt

class MRWordCount(MRJob):

    def mapper(self, _, line):
        yield 'chars', len(line)
        yield 'words', len(line.split())

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRWordCount.run()

