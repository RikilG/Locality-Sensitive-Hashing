"""
Incidence matrix builder for corpus using shingling

This module contains the following functions:
    * list_files - list the files in the given directory
    * get_shingle_matrix - returns incidence-matrix of shingle and documents
"""

from pandas import DataFrame,read_pickle
import multiprocessing as mp
import numpy as np
import codecs
import os


def list_files(folderpath, extension=".txt"):
    """Reads and builds corpus files list from given folderpath

    Parameters
    ----------
    folderpath: str
        The path to target folder where corpus files exist
    extension: str, optional
        File extension of files to be read. Default: .txt
        set to None to read all files
    
    Returns
    -------
    list
        a list of files present in the directory(includes sub-folders)
    """

    print(f"Reading corpus: {folderpath}")
    # check if folder path exists
    if(not os.path.exists(folderpath)):
        raise Exception(f"Given folder path: {folderpath} does not exist")
        return
    
    doc_files = []
    i = 0           # index/id to identify each file

    if extension == None:
        extension = ""
    
    for (root, dirs, files) in os.walk(folderpath):
        for f in files:
            if f.endswith(extension):
                doc_files.append( (os.path.join(root, f), i) )
                i += 1
    
    return doc_files


# for parallel file IO. useful if using a RAID system or disk supports parallel
# reading of files
def parallel_file_read(t_id, file_list, k, output, newline=False):
    """helper-function for parallelism
    """

    for file_tuple in file_list:
        with codecs.open(file_tuple[0], 'r', encoding="utf8", errors='ignore') as doc:
            # print(f"reading: {file_tuple[0]}")
            data = doc.read()
            data = ' '.join(data.split())   # substiture multiples spaces with single space
            data = data.replace('\r\n', ' ') # replace windows line endings with space
            data = data.replace('\r', '')   # remove \r in windows
            data = data.replace('\t', '')   # remove tab-spaces
            if newline is False:
                data.replace('\n', ' ')
            for i in range(0, len(data)-k+1):
                shingle = data[i:i+k]
                output.put((shingle, file_tuple[1]))
        # print(f"Done: {file_tuple}")
    
    output.put(('-1', t_id))


# for parallel file IO. useful if using a RAID system or disk supports parallel
# reading of files
def parallel_adapter(files, k, df, newline, pool=3):
    """helper-function for parallelism
    """
    output = mp.Queue()
    process_files = np.array_split(np.array(files), pool)
    processes = [ mp.Process(target=parallel_file_read, args=(i, process_files[i], k, output, newline)) for i in range(pool) ]

    for p in processes:
        p.start()
    
    x = pool
    while x > 0:
        temp = output.get()
        if df.shape[0]%2000 == 0:
            print(df.shape)
        if temp[0] == '-1':
            x -= 1
            processes[ temp[1] ].join()
            print("thread done")
            continue
        if temp[1] not in df.columns:
            df[ temp[1] ] = 0
        if temp[0] not in df.index:
            df.loc[ temp[0] ] = [ 0 for i in range(df.shape[1]) ]
        df.loc[ temp[0], temp[1] ] = 1
    
    print("Done with all process")
    for p in processes:
        p.join()
    
    return df


def build_matrix(files, k=4, newline=False, parallel=True, pool=3):
    """helper-function: build incidence matrix for k-grams (shingles)
    """

    df = DataFrame()

    if parallel is True:
        return parallel_adapter(files, k, df, newline, pool)

    for f in files:
        with codecs.open(f[0], 'r', encoding="utf8", errors='ignore') as doc:
            print("reading: "+f[0])
            data = doc.read()
            df[ f[1] ] = 0
            
            data = ' '.join(data.split())   # substiture multiples spaces with single space
            data = data.replace('\r\n', ' ') # replace windows line endings with space
            data = data.replace('\r', '')   # remove \r in windows
            data = data.replace('\t', '')   # remove tab-spaces
            if newline is False:
                data = data.replace('\n',' ')
            for i in range(0, len(data)-k+1):
                shingle = data[i:i+k]
                if shingle not in df.index:
                    df.loc[shingle] = [0 for i in range(df.shape[1])]
                    # df = df.append(DataFrame(data={shingle: [0 for i in range(df.shape[1])]}, columns=df.columns))
                df.loc[ shingle, f[1] ] = 1
    
    return df


def get_shingle_matrix(folderpath, shingle_size=8, extension=".txt", parallel=0):
    """Performs shingling and builds incidence index for shingles

    if a already generated pickle file named {foldername}_inc_mat.pickle exists,
    this function will load it instead

    Parameters
    ----------
    folderpath: str
        The path to target folder where corpus files exist
    shingle_size: int, optional
        Size of shingles to divide the documents into. Default: 8
    extension: str, optional
        File extension of files to be read. Default: .txt
        set to None to read all files
    parallel: no of parallel processes to spawn
        Only use if hardware supports parallel read. Else, this does not give
        any speed improvement
    
    Returns
    -------
    pandas.Dataframe
        dataframe containing rows as shingles and cols as doc_ids
    """

    # if pickle file exists, then load and return it instead 
    if os.path.exists(f"{folderpath}_inc_mat.pickle"):
        print(f"Using already created {folderpath}_inc_mat.pickle file")
        incidence_matrix = read_pickle(f"{folderpath}_inc_mat.pickle")
        return incidence_matrix

    # fetch the list of files to be read
    files = list_files(folderpath, extension)
    # check if parallelism is requested
    if parallel == 0:
        incidence_matrix = build_matrix(files, k=shingle_size, parallel=False)
    else:
        incidence_matrix = build_matrix(files, k=shingle_size, parallel=True, pool=parallel)

    print("saving generated incidence index to file...")
    incidence_matrix.to_pickle(f"{folderpath}_inc_mat.pickle")
    print(f"saved to {folderpath}_inc_mat.pickle")
    return incidence_matrix


def main():
    #folderpath = input("Enter folderpath for corpus: ")
    from time import time
    st_time = time()
    folderpath = "corpus"
    incidence_matrix = get_shingle_matrix(folderpath)
    print(f"endtime: {time() - st_time}")
    print(incidence_matrix)


if __name__ == "__main__":
    main()