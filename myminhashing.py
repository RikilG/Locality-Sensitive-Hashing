"""
Reduce the size of shingle incidence matrix by converting docs to signatures

This module calculates the signature of each document using minhashing 
technique by utilizing a number of permutaion hash functions

This module contatins following functions:
    * generate_signature_matrix - to generate signature matrix from incidence 
        matrix
"""

from random import randint
from pandas import DataFrame

def is_prime(n):
    """helper-function: simple primality test
    """

    if n <= 3:
        return n > 1
    elif n%2==0 or n%3==0:
        return False

    i = 5
    while i*i <= n:
        if n%i==0 or n%(i+2)==0:
            return False
        i = i + 6

    return True


def get_next_prime(n):
    """helper-function: gets next nearest prime
    """

    while True:
        n = n + 1
        if is_prime(n):
            return n
        

def generate_random_list(n, max_num):
    """helper-function: generates list of unique random numbers
    """

    rand_list = []
    while n is not 0:
        temp = randint(2, max_num)
        while temp in rand_list:
            temp = randint(3, max_num)
            if temp%2==0:
                temp = temp - 1
        rand_list.append(temp)
        n = n - 1
    return rand_list


def generate_hash_functions(rows, no_of_hash_functions=100):
    """This function generates parameters for given no of hash functions
    
    Parameters
    ----------
    rows: int
        no of shingles in corpus, a.k.a no of rows in shingle matrix
    no_of_hash_functions: int, optional
        no of permutation hash functions to generate for minhashing
        Default: 100
    
    Returns
    -------
    list
        list of functions which can be used as hashes[i](x)
    """

    hashes = []
    c = rows
    if not is_prime(c):
        c = get_next_prime(c)
    a = generate_random_list(no_of_hash_functions, c)
    b = generate_random_list(no_of_hash_functions, c)
    print(a)
    print(b)
    
    # all functions are same here. check this
    for i in range(no_of_hash_functions):
        def hash(x):
            """
            This function calculates hash for given x

            hash function format: (a*x+b)%c where
                c: prime integer just greater than rows
                a,b: random integer less than c
            """
            return (a[i]*x + b[i])%c
        hashes.append(hash)

    return hashes


def generate_signature_matrix(incidence_matrix, no_of_hash_functions=100):
    """This function generates the signature matrix for whole corpus

    Parameters
    ----------
    incidence_matrix: pandas.DataFrame
        incidence index generated after shingling of similar process
    no_of_hash_functions: int, optional
        no of hash functions to use to generate document signatures.
        Default: 100
    
    Returns
    -------
    pandas.DataFrame
        dataframe containing signatures of each document
    """

    rows, cols = incidence_matrix.shape
    hashes = generate_hash_functions(rows, no_of_hash_functions)
    signature_matrix = DataFrame(index=[i for i in range(no_of_hash_functions)], columns=incidence_matrix.columns)

    # for hash_no in range(len(hashes)):
    #     print(f"using hashno: {hash_no}")
    #     temp = {}
    #     for i in range(rows):
    #         temp[ hashes[hash_no](i) ] = i
    #     print(temp)
    #     for doc in range(cols):
    #         for i in range(rows):
    #             if incidence_matrix.iloc[ temp[i] ][doc] == 1:
    #                 signature_matrix.iloc[hash_no][doc] = i
    
    for i in range(rows):
        for j in incidence_matrix.columns:
            if incidence_matrix.iloc[i][j]==1:
                print(f"row: {i}, col: {j}")
                for k in range(no_of_hash_functions):
                    signature_matrix.iloc[k][j] = min(signature_matrix.iloc[k][j], hashes[k](i))
                    print(hashes[k](i))
    
    return signature_matrix