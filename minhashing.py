import pandas as pd
import numpy as np
import sys

def hash_gen(m, n):
    res = []
    for _ in range(n):
        def hash_i(x):
            a = np.random.randint(1, 100)
            b = np.random.randint(-100,100)
            return (a*x+b)%m
        res.append(hash_i)
    return res

def minhash(data):
    m = data.shape[0]
    d = data.shape[1]
    n = 15
    sign_mat = pd.DataFrame(index = [i for i in range(n)], columns = data.columns)
    sign_mat.fillna(sys.maxsize, inplace=True)

    hash_funcs = hash_gen(m, n)

    for i in range(m):
        for j in data.columns:
            if data.loc[data.index[i],j]==1:
                for k in range(n):
                    sign_mat.loc[k,j] = min(sign_mat.loc[k,j], hash_funcs[k](i))

    print(sign_mat)
    return sign_mat