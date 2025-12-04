import connexion
from connexion import NoContent
from datetime import datetime
import time
import yaml
import logging
import logging.config
from pykafka import KafkaClient
import json


with open('/config/app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('/config/log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())

logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')


class KafkaWrapper:
    def __init__(self, hostname, topic):
        self.hostname = hostname
        self.topic = topic
        self.client = None
        self.consumer = None
        self.connect()

    def connect(self):
        """Infinite loop: will keep trying"""
        while True:
            logger.debug("Trying to connect to Kafka...")
            if self.make_client():
                if self.make_consumer():
                    break
            # Sleeps for a random amount of time (0.5 to 1.5s)
            time.sleep(random.randint(500, 1500) / 1000)

    def make_client(self):
        """
        Runs once, makes a client and sets it on the instance.
        Returns: True (success), False (failure)
        """
        if self.client is not None:
            return True

        try:
            self.client = KafkaClient(hosts=self.hostname)
            logger.info("Kafka client created!")
            return True
        except KafkaException as e:
            msg = f"Kafka error when making client: {e}"
            logger.warning(msg)
            self.client = None
            self.consumer = None
            return False

    def make_producer(self):
        """
        Creates a Kafka producer and returns it.
        """
        if self.client is None:
            self.make_client()

        try:
            topic = self.client.topics[self.topic]
            producer = topic.get_sync_producer()
            logger.info("Kafka producer created!")
            return producer
        except KafkaException as e:
            logger.error(f"Kafka producer error: {e}")
            return None

    def messages(self):
        """Generator method that catches exceptions in the consumer loop"""
        if self.consumer is None:
            self.connect()

        while True:
            try:
                for msg in self.consumer:
                    yield msg

            except KafkaException as e:
                msg = f"Kafka issue in consumer: {e}"
                logger.warning(msg)
                self.client = None
                self.consumer = None
                self.connect()

def report_search_readings(body):
    search_report = body["search_readings"]


    for search in search_report:
        trace_id = time.time_ns()
        
        logger.info(f'Received event search_readings with trace id {trace_id}')
        
        data = {
            "trace_id": trace_id,
            "store_id": body["store_id"],
            "store_name": body["store_name"],
            "reporting_timestamp": body["reporting_timestamp"],
            "product_id": search["product_id"],
            "search_count": search["search_count"],
            "recorded_timestamp": search["recorded_timestamp"]
        }

        msg = {
            "type": "search_readings",
            "datetime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "payload": data 
        }

        msg_str = json.dumps(msg)
        producer.produce(msg_str.encode('utf-8'))


        # url = app_config['eventstore1']['url']
        # response = httpx.post(url, json=data)
        
        logger.info(f'Received event search_readings with trace id {trace_id}')
        logger.info(f'Produced message to Kafka topic events')
        
    return NoContent, 201


def report_sold_readings(body):
    purchase_report = body["purchase_readings"]
    for purchase in purchase_report:
        trace_id = time.time_ns()
        
        logger.info(f'Received event purchase_reading with trace id {trace_id}')
        
        data = {
            "trace_id": trace_id,
            "store_id": body["store_id"],
            "store_name": body["store_name"],
            "reporting_timestamp": body["reporting_timestamp"],
            "product_id": purchase["product_id"],
            "purchase_count": purchase["purchase_count"],
            "recorded_timestamp": purchase["recorded_timestamp"]
        }

        msg = {
            "type": "purchase_readings",
            "datetime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "payload": data 
        }

        msg_str = json.dumps(msg)
        producer.produce(msg_str.encode('utf-8'))
        
        # url = app_config['eventstore2']['url']
        # response = httpx.post(url, json=data)
        
        logger.info(f'Received event purchase_readings with trace id {trace_id}')
        logger.info(f'Produced message to Kafka topic events')
        
    return NoContent, 201


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("/config/grocery_api.yml")

if __name__ == "__main__":
    app.run(port=8080, host="0.0.0.0")