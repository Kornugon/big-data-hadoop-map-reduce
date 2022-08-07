from mrjob.job import MRJob
from mrjob.step import MRStep

# terminal:
# python 02_simple_job.py SMSSpamCollection.txt

class MRSimpleJob(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(selfself, _, line):
        yield 'line', 1
        yield 'words', len(line.split())
        yield 'chars', len(line)

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRSimpleJob.run()

