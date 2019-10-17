def band_hashing(band, hash_f, buckets):
    '''
    This function takes a band as input,
    hashes it and puts it in the buckets_list
    at its respective postition.
    '''
    for col in band.columns:
        h = hash_f(tuple(band[col].values))
        if h in buckets: 
            buckets[h].append(col)
        else: 
            buckets[h] = [col]

def lsh(sign_mat, r, hash_f=None):
    '''
    This function uses band_hashing function 
    to hash each band to a bucket in the buckets_list.
    
    buckets_list: a list of dictionaries. Each dictionary 
        contains hashes of column vectors of the band as keys 
        and the list of documents as values.
    b: number of bands
    n: length of a document signature
    r: number of rows in a band
    '''
    n = sign_mat.shape[0]
    b = n//r
    buckets_list = [dict() for i in range(b)]

    if hash_f==None:
        hash_f = hash

    for i in range(0, n-r+1, r):
        band = sign_mat.loc[i:i+r-1,:]
        band_hashing(band, hash_f, buckets_list[int(i/r)])

    return buckets_list

if __name__=='__main__':
    from minhashing import minhash
    from shingling import main
    data = main()
    sign_mat = minhash(data)
