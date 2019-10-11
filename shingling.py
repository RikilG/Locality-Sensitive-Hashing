"""
This module takes the path of corpus, reads the documents and
builds a index for presence of shingles
"""

from pandas import DataFrame,Series
import codecs
import os

def list_files(folderpath):
    """
    This function reads and builds corpus from given folderpath
    """
    print("Reading corpus")
    # check if folder path exists
    if(not os.path.exists(folderpath)):
        print("given folder path does not exist")
        return
    
    doc_files = []
    i = 0
    
    for (root, dirs, files) in os.walk(folderpath):
        for f in files:
            doc_files.append((os.path.join(root, f), i))
            i += 1
    
    return doc_files

def build_matrix(files, k=4, newline=False):
    """
    build incidence matrix for k-grams (shingles)
    """
    df = DataFrame()
    for f in files:
        with codecs.open( f[0],'r', encoding="utf8",errors='ignore' ) as doc:
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

def main():
    files = list_files("corpus")
    incidence_matrix = build_matrix(files, k=8)
    print(incidence_matrix)

if __name__ == "__main__":
    main()