"""
Incidence matrix builder for corpus using shingling

This module contains the following functions:
    * list_files - list the files in the given directory
    * get_shingle_matrix - returns incidence-matrix of shingle and documents
"""

from pandas import DataFrame,Series
import codecs
import os


def list_files(folderpath):
    """
    Reads and builds corpus files list from given folderpath

    Parameters
    ----------
    folderpath: str
        The path to target folder where corpus files exist
    
    Returns
    -------
    list
        a list of files present in the directory(includes sub-folders)
    """

    print("Reading corpus")
    # check if folder path exists
    if(not os.path.exists(folderpath)):
        raise Exception(f"Given folder path: {folderpath} does not exist")
        return
    
    doc_files = []
    i = 0           # index/id for each file
    
    for (root, dirs, files) in os.walk(folderpath):
        for f in files:
            if f.endswith(".txt"):
                doc_files.append( (os.path.join(root, f), i) )
                i += 1
    
    return doc_files


def build_matrix(files, k=4, newline=False):
    """
    helper-function: build incidence matrix for k-grams (shingles)
    """

    df = DataFrame()
    for f in files:
        with codecs.open(f[0], 'r', encoding="utf8", errors='ignore') as doc:
            print("reading: "+f[0])
            str = doc.read()
            df[ f[1] ] = 0
            # print(df)
            if newline is False:
                str = str.replace('\n',' ')
            for i in range(0, len(str)-k+1):
                shingle = str[i:i+k]
                if shingle not in df.index:
                    df.loc[shingle] = [0 for i in range(df.shape[1])]
                    # df = df.append(DataFrame(data={shingle: [0 for i in range(df.shape[1])]}, columns=df.columns))
                df.loc[ shingle, f[1] ] = 1
    
    return df


def get_shingle_matrix(folderpath, shingle_size=8):
    """
    Performs shingling and builds incidence index for shingles

    Parameters
    ----------
    folderpath: str
        The path to target folder where corpus files exist
    shingle_size: int
        Size of shingles to divide the documents into
    
    Returns
    -------
    Dataframe
        pandas dataframe containing rows as shingles and cols as doc_ids
    """

    files = list_files(folderpath)
    incidence_matrix = build_matrix(files, k=shingle_size)
    return incidence_matrix


def main():
    folderpath = input("Enter folderpath for corpus: ")
    incidence_matrix = get_shingle_matrix(folderpath)
    print(incidence_matrix)


if __name__ == "__main__":
    main()