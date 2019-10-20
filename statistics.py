import numpy as np

def jaccard(x, a, signature_matrix):
    x = set(signature_matrix[x])
    a = set(signature_matrix[a])
    return len(x & a)/len(x | a)


def euclid(x, a, signature_matrix):
    x = signature_matrix[x]
    a = signature_matrix[a]
    return np.sum(a**2 - x**2)**0.5

def cosine(x, a, signature_matrix):
    x = signature_matrix[x]
    a = signature_matrix[a]
    return np.dot(a,x)/(np.sum(a**2) * np.sum(x**2))**0.5


def compute_similarity(x, similar_docs, signature_matrix, sim_type="jaccard"):
    
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
    req = [ i for f, i in output if i>=threshold ]
    return len(req)/len(output)


def recall(threshold, x, size, output, signature_matrix, sim_type):
    docs = compute_similarity(x, [ i for i in range(size) ], signature_matrix, sim_type)
    print(docs)
    req = [ i for f, i in output if i>=threshold ]
    den = [ i for f, i in docs if i>=threshold and f!=x ]
    return len(req)/len(den)


def get_file_name(file_id, files):
    for filename, f_id in files:
        if file_id == f_id:
            return filename
