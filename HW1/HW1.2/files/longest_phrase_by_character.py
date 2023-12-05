
from mrjob.job import MRJob
from mrjob.step import MRStep

class LongestPhraseByCharacter(MRJob):

    def mapper_extract_phrases(self, _, line):
        fields = line.strip().split('"')
        if len(fields) == 7:
            character, phrase = fields[3], fields[5]
            yield (character, phrase)

    def combiner_find_longest_phrase(self, character, phrases):
        longest_phrase = max(phrases, key=len)
        yield (character, longest_phrase)

    def reducer_find_longest_phrase(self, character, phrases):
        longest_phrase = max(phrases, key=len)
        yield None, (character, longest_phrase)

    def reducer_sort_and_output(self, _, character_longest_phrases):
        sorted_phrases = sorted(character_longest_phrases, key=lambda x: len(x[1]), reverse=True)
        for character, phrase in sorted_phrases:
            yield character, phrase

    def steps(self):
        return [
            MRStep(mapper=self.mapper_extract_phrases,
                   combiner=self.combiner_find_longest_phrase,
                   reducer=self.reducer_find_longest_phrase),
            MRStep(reducer=self.reducer_sort_and_output)
        ]

if __name__ == '__main__':
    LongestPhraseByCharacter.run()
