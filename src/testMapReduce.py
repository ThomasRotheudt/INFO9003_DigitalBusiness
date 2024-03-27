import re
from functools import reduce

def map_function(word):
    return (word, 1)

def reduce_function(acc, item):
    acc[item[0]] += item[1]
    return acc

def word_counter(text):
    words = [re.sub(r'\W+', ' ', word).lower() for word in re.split(r'\s+', text) if word]
    mapped_words = list(map(map_function, words))
    return reduce(reduce_function, mapped_words, dict.fromkeys(words, 0))

text = "This is a sample text. This text is for demonstrating the word counter program. The program counts the frequency of a word in a given text using map reduce."

word_count = word_counter(text)

for word, count in word_count.items():
    print(f"{word}: {count}")