
from mrjob.job import MRJob
from mrjob.step import MRStep

import nltk
from nltk.tokenize import word_tokenize
from nltk.util import ngrams
import string

nltk.download('punkt')
nltk.download('stopwords')

class NumberOfBigrams(MRJob):
    
    def mapper_preprocess_text(self, _, line):
        fields = line.strip().split('"')
        if len(fields) == 7:
            text = fields[5]
            
            text = text.lower()
            text = text.translate(str.maketrans(", ", len(", ")*" "))
            text = text.translate(str.maketrans("", "", string.punctuation))

            tokens = word_tokenize(text)
            stopwords = set(nltk.corpus.stopwords.words('english'))
            filtered_tokens = [token for token in tokens if token not in stopwords]
            
            bigrams = ngrams(filtered_tokens, 2)
            for bigram in bigrams:
                yield (bigram, 1)
    
    def combiner_count_bigrams(self, bigram, counts):
        yield (bigram, sum(counts))
        
    def reducer_count_bigrams(self, bigram, counts):
        yield None, (bigram, sum(counts))
        
    def reducer_find_top_20_bigrams(self, _, bigram_counts):
        top_20_bigrams = sorted(bigram_counts, key=lambda x: x[1], reverse=True)[:20]
        for bigram, count in top_20_bigrams:
            bigram_str = ' '.join(bigram)
            yield bigram_str, count
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_preprocess_text,
                   combiner=self.combiner_count_bigrams,
                   reducer=self.reducer_count_bigrams),
            MRStep(reducer=self.reducer_find_top_20_bigrams)
        ]
    
if __name__ == '__main__':
    NumberOfBigrams().run()
