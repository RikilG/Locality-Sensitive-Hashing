"""
Incidence matrix builder for corpus using shingling

This module contains the following functions:
    * list_files - list the files in the given directory
    * get_shingle_matrix - returns incidence-matrix of shingle and documents
"""

from pandas import DataFrame,read_pickle
import numpy as np
import codecs
import os
import pickle
from tqdm import tqdm


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
    
    Raises
    ------
    Exception
        if given folder path does not exist
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

def build_matrix(files, k=4, newline=False):
    """helper-function: build incidence matrix for k-grams (shingles)
    """

    df = DataFrame(columns=[x[1] for x in files])

    for f in tqdm(files):
        with codecs.open(f[0], 'r', encoding="utf8", errors='ignore') as doc:
            # print("reading: "+f[0])
            data = doc.read()
            # df[ f[1] ] = 0
            data = data.lower()             # lowercase all letters
            data = ' '.join(data.split())   # substiture multiples spaces with single space
            data = data.replace('\r\n', ' ') # replace windows line endings with space
            data = data.replace('\r', '')   # remove \r in windows
            data = data.replace('\t', '')   # remove tab-spaces
            if newline is False:
                data = data.replace('\n',' ')

            # st_time = time.time()
            for i in range(0, len(data)-k+1):
                shingle = data[i:i+k]
                if (shingle in df.index) == False:
                    df.loc[shingle] = [0 for i in range(df.shape[1])]
                df.at[ shingle, f[1] ] = 1
            # print(time.time()-st_time)
    
    return df


def get_shingle_matrix(folderpath, shingle_size=8, extension=".txt"):
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
    incidence_matrix = None
    if os.path.exists(f"{folderpath}_inc_mat.pickle"):
        incidence_matrix = read_pickle(f"{folderpath}_inc_mat.pickle")
        if os.path.exists("file_list.pickle"):
            print(f"Using already created {folderpath}_inc_mat.pickle file")
            print("using pickled file list")
            with open("file_list.pickle", 'rb') as file_list_pkl:
                files = pickle.load(file_list_pkl)
            return incidence_matrix, files
        print("file list not found")

    # fetch the list of files to be read
    files = list_files(folderpath, extension)
    # check if parallelism is requested
    incidence_matrix = build_matrix(files, k=shingle_size)

    print("saving generated incidence index to file...")
    incidence_matrix.to_pickle(f"{folderpath}_inc_mat.pickle")
    with open("file_list.pickle", 'wb') as file_list_pkl:
        pickle.dump(files, file_list_pkl)
    print(f"saved to {folderpath}_inc_mat.pickle")
    return incidence_matrix, files


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