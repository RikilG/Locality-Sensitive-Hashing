import shingling
import minhashing

print("\n*** Plagiarism detection using LSH ***\n")

# step 1: shingling
folderpath = "temp"
shingle_size = 8
shingle_matrix = shingling.get_shingle_matrix(folderpath, 8, extension=None)
print(shingle_matrix)

# step 2: min-hashing
no_of_hash_functions = 5
signature_matrix = minhashing.generate_signature_matrix(shingle_matrix, no_of_hash_functions)
print(signature_matrix)

# step 3: LSH(Locality sensitive hashing)

print("\n*** End of Program ***\n")