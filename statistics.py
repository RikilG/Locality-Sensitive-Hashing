"""
includes functions (metrics) required to evaluate Locality Sensitive Hashing.
"""
import numpy as np

def jaccard(x, a, signature_matrix):
    """This function finds jaccard similarity between two documents

    Parameters
    ----------
    x: int
        1-d signature array of document with docid=x
    a: int
        1-d signature array of document with docid=a
    signature_matrix: pandas DataFrame
        contains signature vectors of all documents as columns
    
    Returns
    -------
    int
        jaccard similarity between documents x and a
    """
    x = set(signature_matrix[x])
    a = set(signature_matrix[a])
    return len(x & a)/len(x | a)


def euclid(x, a, signature_matrix):
    """This function finds euclidean similarity between two documents

    Parameters
    ----------
    x: int
        1-d signature array of document with docid=x
    a: int
        1-d signature array of document with docid=a
    signature_matrix: pandas DataFrame
        contains signature vectors of all documents as columns
    
    Returns
    -------
    int
        euclidean distance between documents x and a
    """
    x = signature_matrix[x]
    a = signature_matrix[a]
    return np.sum(a**2 - x**2)**0.5

def cosine(x, a, signature_matrix):
    """This function finds cosine similarity between two documents

    Parameters
    ----------
    x: int
        1-d signature array of document with docid=x
    a: int
        1-d signature array of document with docid=a
    signature_matrix: pandas DataFrame
        contains signature vectors of all documents as columns
    
    Returns
    -------
    int
        cosine similarity between documents x and a
    """
    x = signature_matrix[x]
    a = signature_matrix[a]
    return np.dot(a,x)/(np.sum(a**2) * np.sum(x**2))**0.5


def compute_similarity(x, similar_docs, signature_matrix, sim_type="jaccard"):
    """This function finds cosine similarity between two documents

    Parameters
    ----------
    x: int
        1-d signature array of document with docid=x
    similar_docs: list
        a list of docids which are similar to x.
    signature_matrix: pandas DataFrame
        contains signature vectors of all documents as columns
    sim_type: string
        can take values jaccard, euclid, cosine. 

    Returns
    -------
    list
        sorted list of (docid, score) tuples.
    """
    if sim_type == "jaccard": sim_fun = jaccard
    elif sim_type == "euclid": sim_fun = euclid
    elif sim_type == "cosine": sim_fun = cosine
    # write for all other funcs
    ranked_list = []
    for i in similar_docs:
        if i == x: continue
        score = sim_fun(x, i, signature_matrix)
        ranked_list.append((i, score))
    
    if sim_type == "euclid":
        return sorted(ranked_list, key=lambda x: x[1], reverse=False)
    else:
        return sorted(ranked_list, key=lambda x: x[1], reverse=True)


def precision(threshold, output):
    """This function finds cosine similarity between two documents

    Parameters
    ----------
    threshold: float
        value of similarity above which retrieved docs are considered relevant
    output: list
        a list of retrieved items.

    Returns
    -------
    float
        precision value for the given set of retrieved items.
    """
    req = [ i for f, i in output if i>=threshold ]
    return len(req)/len(output)


def recall(threshold, x, size, output, signature_matrix, sim_type):
    """This function finds cosine similarity between two documents

    Parameters
    ----------
    threshold: float
        value of similarity above which retrieved docs are considered relevant
    x: int
        1-d signature array of document with docid=x
    size: int
        number of all documents in the corpus
    output: list
        a list of retrieved items.
    signature_matrix: pandas DataFrame
        contains signature vectors of all documents as columns
    sim_type: string
        can take values jaccard, euclid, cosine. 

    Returns
    -------
    float
        recall value for the given set of retrieved items.
    """
    docs = compute_similarity(x, [ i for i in range(size) ], signature_matrix, sim_type)
    print(docs)
    req = [ i for f, i in output if i>=threshold ]
    den = [ i for f, i in docs if i>=threshold and f!=x ]
    return len(req)/len(den)


def get_file_name(file_id, files):
    """This function finds cosine similarity between two documents

    Parameters
    ----------
    threshold: float
        value of similarity above which retrieved docs are considered relevant
    files: list
        a list of tuples containing filename and file id.

    Returns
    -------
    string
        name of the file with given file_id.
    """
    for filename, f_id in files:
        if file_id == f_id:
            return filename
