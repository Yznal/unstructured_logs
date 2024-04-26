# SPDX-License-Identifier: MIT

import json
import time
import logging
import sys
import regex as re
import os
import pandas as pd
import clickhouse_connect
from os.path import dirname

from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig

# persistence_type = "NONE"
# persistence_type = "REDIS"
# persistence_type = "KAFKA"
persistence_type = "FILE"

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')

#client = clickhouse_connect.get_client(host='localhost', username='default', password='')
#client.command('CREATE TABLE new_table (key UInt32, value String, metric Float64) ENGINE MergeTree ORDER BY key')
#row1 = [1000, 'String Value 1000', 5.233]
#row2 = [2000, 'String Value 2000', -107.04]
#data = [row1, row2]
#client.insert('new_table', data, column_names=['key', 'value', 'metric'])

def follow(thefile):
    thefile.seek(0,2)
    while True:
        line = thefile.readline()
        if not line:
            time.sleep(0.1)
            continue
        yield line

if persistence_type == "KAFKA":
    from drain3.kafka_persistence import KafkaPersistence

    persistence = KafkaPersistence("drain3_state", bootstrap_servers="localhost:9092")

elif persistence_type == "FILE":
    from drain3.file_persistence import FilePersistence

    persistence = FilePersistence("drain3_state.bin")

elif persistence_type == "REDIS":
    from drain3.redis_persistence import RedisPersistence

    persistence = RedisPersistence(redis_host='',
                                   redis_port=25061,
                                   redis_db=0,
                                   redis_pass='',
                                   is_ssl=True,
                                   redis_key="drain3_state_key")
else:
    persistence = None

config = TemplateMinerConfig()
config.load(f"{dirname(__file__)}/drain3.ini")
config.profiling_enabled = False

log_format = "<Date> <Level> <Pid> --- \[<Thread>\] <Logger> : <Content>"
path = "../../../spring-petclinic/spring.log"
input_dir  = '../../../spring-petclinic/' # The input directory of log file
output_dir = 'demo_result/'  # The output directory of parsing results
log_file   = 'spring.log'  # The input log file name

template_miner = TemplateMiner(persistence, config, log_format, log_file, output_dir)
print(f"Drain3 started with '{persistence_type}' persistence")
print(f"{len(config.masking_instructions)} masking instructions are in use")
print(f"Starting training mode. Reading from std-in ('q' to finish)")

logfile = open(os.path.join(input_dir, log_file),"r")
loglines = follow(logfile)
for log_line in loglines:
    result = template_miner.add_log_message(log_line)
    result_json = json.dumps(result)
    print(result_json)
    template = result["template_mined"]
    params = template_miner.extract_parameters(template, log_line, exact_matching=False)
    print(f"Parameters: {str(params)}")
    

print("Training done. Mined clusters:")
for cluster in template_miner.drain.clusters:
    print(cluster)

print(f"Starting inference mode, matching to pre-trained clusters. Input log lines or 'q' to finish")
while True:
    log_line = input("> ")
    if log_line == 'q':
        break
    cluster = template_miner.match(log_line)
    if cluster is None:
        print(f"No match found")
    else:
        template = cluster.get_template()
        print(f"Matched template #{cluster.cluster_id}: {template}")
        print(f"Parameters: {template_miner.get_parameter_list(template, log_line)}")
