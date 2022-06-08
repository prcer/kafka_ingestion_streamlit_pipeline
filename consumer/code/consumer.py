import time
import os
import math
from confluent_kafka import DeserializingConsumer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import logging as log
import json
import pgeocode
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import threading
from provider_service import ProviderService, SCHEMA_STR
from postgres_wrapper import connect, insert_tuple, setup_table

log.basicConfig(filename = "/var/log/consumer.log",
                filemode = "w",
                format='%(asctime)s - %(levelname)s: %(message)s',
                level = log.DEBUG)

MAX_WORKERS = int(os.environ['MAX_WORKERS'])

class ScienceConsumer:
    def __init__(self):
        log.info('Starting constructor setup')
        self.topic = [os.environ['KAFKA_TOPIC']]
        sr_conf = {'url': os.environ['SCHEMA_REGISTRY']}
        schema_registry_client = SchemaRegistryClient(sr_conf)
        avro_deserializer = AvroDeserializer(schema_registry_client,
                                             SCHEMA_STR)
        # TODO Review kafka settings
        conf = {'bootstrap.servers': os.environ['KAFKA_BROKER'],
                'group.id': f'{self.topic}_grp',
                'enable.auto.commit': True,
                'value.deserializer': avro_deserializer,
                'auto.offset.reset': 'earliest'}    
        self.consumer = DeserializingConsumer(conf)
        self.pg_engine = connect(os.environ['POSTGRES_DB_URL'])
        setup_table(self.pg_engine)
        self.schema = os.environ['POSTGRES_SCHEMA']
        self.table = os.environ['POSTGRES_TABLE']
        self.nomi = pgeocode.Nominatim('us') # for geo localization
        self.msgs_consumed = 0
        self.msgs_processed_success = 0
        self.msgs_skipped = 0
        self.active_workers = 0

        # Use a thread pool to process msg due the I/O bound nature of postgres insertion
        self.threadpool = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        self.lock = Lock()

        log.info('Finished constructor setup')
        
    
    def process_msg(self, msg):
        try:
            idx = threading.current_thread().getName().split('_')[-1]

            msg_payload_dict = msg.value()
            log.info(f'{idx} - Processing msg: {msg_payload_dict}')
            # Insert only providers based on US because they account for 99.99% of providers 
            #   and to avoid a large, sparse map visual.
            # This is also important to be able to breakdown metric by US states properly. 
            if msg_payload_dict['Rndrng_Prvdr_Cntry'] != 'US':
                with self.lock:
                    self.msgs_skipped += 1
                log.info(f'{idx} - Skipping msg because it is not US country')
                return

            prov_svc = ProviderService(msg_payload_dict, self.nomi)
            if not math.isnan(prov_svc.latitude) and not math.isnan(prov_svc.longitude):
                prov_svc_tuple = prov_svc.to_tuple()
                if insert_tuple(self.pg_engine, self.schema, self.table, prov_svc_tuple, idx):
                    with self.lock:
                        self.msgs_processed_success += 1
                    log.info(f'{idx} - Finished processing msg successfully')
                else:
                    log.error(f'{idx} - Failed to insert tuple into PG: {prov_svc_tuple} | RAW MSG: {msg_payload_dict}')
            else:
                # A small number of rows will fail to ingest due to latitude/longitude being "nan", because country is 'US', although
                    #  the ZIP code is in a different country.
                with self.lock:
                    self.msgs_skipped += 1
                log.info(f'{idx} -Skipping msg with incorrect ZIP code outside US, even though the country is specified as US in record | RAW MSG: {msg_payload_dict}')
            
        except Exception as e:
            log.error(f'{idx} - Failed to process msg: {str(e)}')
        finally:
            with self.lock:
                self.active_workers -= 1


    def consume_loop(self):
        try:
            self.consumer.subscribe(self.topic)
            log.info(f'Subscribed to topic {self.topic}')
        except Exception as e:
            log.error(f'Failed to subscribe to topic: {str(e)}')
        # TODO Define graceful and error exit conditions
        # TODO Handle specific exceptions
        while True:
            try:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None: 
                    continue
                if msg.error():
                    log.error(msg.error())
                    raise KafkaException(msg.error())
                else:
                    self.msgs_consumed += 1
                    while self.active_workers >= MAX_WORKERS:
                        time.sleep(0.1)
                    with self.lock:
                        self.active_workers += 1
                    # Let separated thread process the message
                    self.threadpool.submit(self.process_msg, msg)
                if self.msgs_consumed % 1000 == 0:
                    log.info(f'Messages consumed: {self.msgs_consumed} | Fully Processed: {self.msgs_processed_success} | Skipped: {self.msgs_skipped}')
            except Exception as e:
                log.error(f'Exception in consume loop: {str(e)}')
        # TODO currently unreachable: close consumer, close postgres engine, shutdown threadpool
            
                 
if __name__ == '__main__':
    log.info(f'Waiting for kafka and postgres to be available')
    # TODO Check for broker and 'science' topic availability, workaround is to sleep for 2min30s before startup
    # TODO Check for postgres availability, workaround is to sleep for 2min30s before startup
    time.sleep(150)
    science = ScienceConsumer()
    science.consume_loop()
    