import codecs
import os
from datetime import datetime
import re

tbeg = datetime.now()

#files_list = ["author.csv","book2subjects.csv","subject.csv","sub_subject.csv","sub_sub_subject.csv","book.csv"]
files_list = ["test.csv"]

def clean_file(fname):
    print("Processing ",fname)
    fin = codecs.open(fname,"r",encoding='utf-16-le',errors='ignore')
    fout = codecs.open("cleaned_"+fname,"w",encoding='utf-8')

    cnt = 0
    cleaned = []

    for line in fin:
        cnt += 1
        decoded_str = line.replace("\x00","")
        cleaned.append(decoded_str)
        if cnt > 0 and (cnt % 500000 == 0):
            print(cnt)
            fout.writelines(cleaned)
            cleaned = []

    if len(cleaned) > 0:
        fout.writelines(cleaned)

    fout.close()
    fin.close()
    print("done")

for infile in files_list:
    clean_file(infile)

tsecs = (datetime.now() - tbeg).total_seconds()
print("total seconds it took to process: {} seconds".format(tsecs))
