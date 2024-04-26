#!/usr/bin/env python

import sys
sys.path.append('../../')
from Drain import LogParser

input_dir  = '../../../spring-petclinic/' # The input directory of log file
output_dir = 'demo_result/'  # The output directory of parsing results
log_file   = 'spring.log'  # The input log file name
log_format = "<Date> <Level> <Pid> --- \[<Thread>\] <Logger> : <Content>"  # HDFS log format
# Regular expression list for optional preprocessing (default: [])
regex      = []
st         = 0.5  # Similarity threshold
depth      = 4  # Depth of all leaf nodes

parser = LogParser(log_format, indir=input_dir, outdir=output_dir,  depth=depth, st=st, rex=regex)
parser.parse(log_file)
