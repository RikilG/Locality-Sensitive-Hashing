"""
This program aims to detect plagiarism using Locality Sensitive Hahsing(LSH)

Process:
    - shingling: converting documents into set of tokens. each token is called 
        as shingle which is a sequence of characters present in file. This process
        generates a sparse incidence matrix
    - minhashing: as incidence matrix is sparse, this step aims to reduce the 
        size of matrix by producing signature for each document present in the 
        incidence matrix
    - lsh: dividing signature matrix into horizontal bands and applying hash
        functions on them to determing which documents fall in same bucket

NOTE: if pickle(binary) files exist for each operation, they will be used 
    instead of generating them again(to save time). rename or delete the file
    to start afresh.
"""

import time
import shingling
import minhashing

print("\n*** Plagiarism detection using LSH ***\n")

# step 1: shingling
timer_start = time.time()   # start timer
folderpath = "corpus"       # path to corpus
extension=".txt"            # specified extensions to read. Set to None to ignore extension
shingle_size = 8            # size of shingle: 8-12 is reommended
shingle_matrix = shingling.get_shingle_matrix(folderpath, 8, extension)
print(f"Time taken for shingling: {time.time()-timer_start}")

# step 2: min-hashing
start_time = time.time()    # start timer
no_of_hash_functions = 200  # specify no of hash functions for signature matrix
signature_matrix = minhashing.generate_signature_matrix(shingle_matrix, no_of_hash_functions)
print(f"Time taken for minhashing: {time.time()-timer_start}")

# print to debug
# print(shingle_matrix)
# print(shingle_matrix.shape)
# print(signature_matrix)
# print(signature_matrix.shape)

# step 3: LSH(Locality sensitive hashing)

print("\n*** End of Program ***\n")