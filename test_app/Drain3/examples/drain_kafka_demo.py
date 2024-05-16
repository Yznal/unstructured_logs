# SPDX-License-Identifier: MIT

import json
import time
import logging
import sys
import os
from kafka import KafkaConsumer
from os.path import dirname

from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig

# persistence_type = "NONE"
# persistence_type = "REDIS"
# persistence_type = "KAFKA"
persistence_type = "FILE"

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')


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
output_dir = 'demo_result/'  # The output directory of parsing results
log_file   = 'spring.log'  # The input log file name

headers_mapping = {"Timestamp": "Date",
                   "Level": "Level",
                   "Content": "Content"}
time_format = "%Y-%m-%dT%H:%M:%S.%f%z"

template_miner = TemplateMiner(persistence_handler=persistence, config=config, log_format=log_format,
                               log_file=log_file, output_dir=output_dir, clickhouse=True,
                               headers_mapping=headers_mapping, time_format=time_format)
print(template_miner.path)
print(f"Drain3 started with '{persistence_type}' persistence")
print(f"{len(config.masking_instructions)} masking instructions are in use")
print(f"Starting parsing.")
print(os.getcwd())

consumer = KafkaConsumer('messages',
                         bootstrap_servers='host.docker.internal:29092',
                         auto_offset_reset='latest',
                         enable_auto_commit=False,
                         api_version=(0, 10, 2),
                         value_deserializer=lambda m: json.loads(m.decode('ascii'))
                        )

for message in consumer:
    result = template_miner.add_log_message(message.value['message'])
    result_json = json.dumps(result)
    print(result_json)
    template = result["template_mined"]
    params = template_miner.extract_parameters(template, message.value['message'], exact_matching=False)
    print(f"Parameters: {str(params)}")
    
    

