import random
import os

def load_word_list(filename):
    """Load words from a given file into a list."""
    with open(filename, 'r') as file:
        return [line.strip() for line in file]

def create_file_with_words(filename, words, size_in_mb=16):
    """Create a file with words from the list until it reaches approximately size_in_mb."""
    size_in_bytes = size_in_mb * 1024 * 1024
    current_size = 0

    with open(filename, 'w') as file:
        while current_size < size_in_bytes:
            word = random.choice(words) + ' '
            word_size = len(word.encode('utf-8'))
            if word_size + current_size > size_in_bytes:
                break
            file.write(word)
            current_size += len(word.encode('utf-8'))

word_list = load_word_list('wordlist.txt')  # Make sure you have 'wordlist.txt' in the same directory

for i in range(1, 6):
    file_name = f'input/real_words_{i}.txt'
    create_file_with_words(file_name, word_list)
    print(f'{file_name} created')

print("All files created.")
