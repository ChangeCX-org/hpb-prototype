from datetime import datetime
import boto3
import os

BUCKET_NAME = 'hpbfull'
KEY_PREFIX = 'processed_files/run2cur/'

s3 = boto3.client('s3')

RUN = 3
BATCH_LINES = 10000
TOTAL_LINES = BATCH_LINES * 2000
file_num = 0
outbuf = []
outfile_name = "stitched.json"
infile_pattern = "run-DataSink0-{}-part-r-{:05d}".format(RUN,file_num)
print("Processing file: ",infile_pattern)
outfile = open(outfile_name,"w")

try:
    s3.download_file(BUCKET_NAME, KEY_PREFIX+infile_pattern, infile_pattern)
    infile = open(infile_pattern,"r")
except:
    infile = None
    print("File {} does not exist, check RUN number".format(infile_pattern))

linecount = 0
total_lines = 0

while infile:
    for line in infile:
        outbuf.append(line)
        linecount += 1
        total_lines += 1
        if linecount % BATCH_LINES == 0:
            outfile.writelines(outbuf)
            outbuf = []
            linecount = 0

        if total_lines >= TOTAL_LINES:
            infile.close()
            os.remove(infile_pattern)
            infile = None
            outbuf = []
            break

    if not infile:
        print("Line Count: ",linecount)
        print("Total Lines: ",total_lines)
        continue
    infile.close()
    os.remove(infile_pattern)
    file_num += 1
    infile_pattern = "run-DataSink0-{}-part-r-{:05d}".format(RUN,file_num)
    print("Processing file: ",infile_pattern)
    try:
        s3.download_file(BUCKET_NAME, KEY_PREFIX+infile_pattern, infile_pattern)
        infile = open(infile_pattern,"r")
        outbuf.append("\n")
    except:
        infile = None

if len(outbuf):
    outfile.writelines(outbuf)

outfile.close()
print("Done")    

