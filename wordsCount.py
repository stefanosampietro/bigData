from mrjob.job import MRJob
from mrjob.step import MRStep

class wordCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   reducer=self.reducer_count_words)
        ]

    def mapper_get_words(self, _, line):
        parole = line.split(' ')
        for parola in parole :
            yield parola, 1

    def reducer_count_words(self, parola, values):
        yield parola, sum(values)

if __name__ == '__main__':
   wordCount.run()