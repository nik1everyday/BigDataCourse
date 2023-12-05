# %%file is an Ipython magic function that saves the code cell as a file

from mrjob.job import MRJob

class DialogueCount(MRJob):

    def mapper(self, _, line):
        fields = line.split('"')
        
        if len(fields) == 7:
            character = fields[3]
            yield (character, 1)

    def reducer(self, character, counts):
        yield (character, sum(counts))

if __name__ == "__main__":
    DialogueCount.run()
