import json
import time
import logging
import sys
import regex as re
import os
import pandas as pd
from os.path import dirname

from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver as Observer

from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig

import threading
import copy

lock = threading.Lock()

PATH =  "../../../spring-petclinic/spring.log"

def follow_for_thread(path):
    cur_path = copy.copy(path)
    while True:
        with open(cur_path, 'r') as file:
            file.seek(0,2)
            while True:
                with lock:
                    if PATH != cur_path:
                        cur_path = PATH
                        break
                log_line = file.readline()
                if not log_line:
                    time.sleep(0.1)
                    continue
                result = template_miner.add_log_message(log_line)
                result_json = json.dumps(result)
                print(result_json)
                template = result["template_mined"]
                params = template_miner.extract_parameters(template, log_line, exact_matching=False)
                print(f"Parameters: {str(params)}")


class CustomForThread(FileSystemEventHandler):
    def __init__(self):
        self.loglines = None

    def on_created(self, event):
        with lock:
            super().on_created(event)
            if not event.is_directory:
                    global PATH
                    PATH = event.src_path

def check_new_file(input_dir):
    event_handler = CustomForThread()
    observer = Observer()
    observer.schedule(event_handler, input_dir, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(0)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()


# persistence_type = "NONE"
# persistence_type = "REDIS"
# persistence_type = "KAFKA"
persistence_type = "FILE"

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')


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
input_dir  = '../../../spring-petclinic' # The input directory of log file
output_dir = 'demo_result/'  # The output directory of parsing results
log_file   = 'spring.log'  # The input log file name

template_miner = TemplateMiner(persistence_handler=persistence, config=config, log_format=log_format, log_file=log_file, output_dir=output_dir, clickhouse=False)
print(template_miner.path)
print(f"Drain3 started with '{persistence_type}' persistence")
print(f"{len(config.masking_instructions)} masking instructions are in use")
print(f"Starting parsing.")

threading.Thread(target=follow_for_thread, args=[PATH]).start()
threading.Thread(target=check_new_file, args=[input_dir]).start()

