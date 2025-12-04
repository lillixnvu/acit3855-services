import connexion
from connexion import NoContent
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, select
from models import SearchReading, PurchaseReading
import yaml
import logging
import logging.config
from pykafka import KafkaClient
from pykafka.common import OffsetType
from threading import Thread
import json
import time
import random
from pykafka.exceptions import KafkaException
import logging

with open('/config/app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('/config/log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    
logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

db_conf = app_config['datastore']
user = db_conf['user']
password = db_conf['password']
hostname = db_conf['hostname']
port = db_conf['port']
db = db_conf['db']

ENGINE = create_engine(
    f"mysql+pymysql://{user}:{password}@{hostname}:{port}/{db}",
    pool_size=5,
    pool_recycle=280,
    pool_pre_ping=True
)

def make_session():
    return sessionmaker(bind=ENGINE)()


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

    def make_consumer(self):
        """
        Runs once, makes a consumer and sets it on the instance.
        Returns: True (success), False (failure)
        """
        if self.consumer is not None:
            return True

        if self.client is None:
            return False

        try:
            topic = self.client.topics[self.topic]
            self.consumer = topic.get_simple_consumer(
                reset_offset_on_start=False,
                auto_offset_reset=OffsetType.LATEST
            )
            return True
        except KafkaException as e:
            msg = f"Make error when making consumer: {e}"
            logger.warning(msg)
            self.client = None
            self.consumer = None
            return False

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


def get_search_readings(start_timestamp, end_timestamp):
    session = make_session()

    start = datetime.strptime(start_timestamp.rstrip('Z'), "%Y-%m-%dT%H:%M:%S.%f")
    end = datetime.strptime(end_timestamp.rstrip('Z'), "%Y-%m-%dT%H:%M:%S.%f")
    
    statement = select(SearchReading).where(
        SearchReading.date_created >= start
    ).where(
        SearchReading.date_created < end
    )

    results = [result.to_dict() for result in session.execute(statement).scalars().all()]
    session.close()

    return results, 200

def get_purchase_readings(start_timestamp, end_timestamp):
    session = make_session()

    start = datetime.strptime(start_timestamp.rstrip('Z'), "%Y-%m-%dT%H:%M:%S.%f")
    end = datetime.strptime(end_timestamp.rstrip('Z'), "%Y-%m-%dT%H:%M:%S.%f")

    statement = select(PurchaseReading).where(
        PurchaseReading.date_created >= start
    ).where(
        PurchaseReading.date_created < end
    )

    results = [result.to_dict() for result in session.execute(statement).scalars().all()]
    session.close()

    logger.info(f"Found {len(results)} purchase readings between {start} and {end}")

    return results, 200


def process_messages():
    hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    kafka_wrapper = KafkaWrapper(hostname, b"events")

    for msg in kafka_wrapper.messages():
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        logger.info(f"Message: {msg}")
        payload = msg["payload"]

        if msg["type"] == "search_readings":
            session = make_session()
            sr = SearchReading(
                trace_id=payload["trace_id"],
                store_id=payload["store_id"],
                store_name=payload["store_name"],
                product_id=payload["product_id"],
                search_count=payload["search_count"],
                recorded_timestamp=payload["recorded_timestamp"]
            )
            session.add(sr)
            session.commit()
            session.close()

        elif msg["type"] == "purchase_readings":
            session = make_session()
            pr = PurchaseReading(
                trace_id=payload["trace_id"],
                store_id=payload["store_id"],
                store_name=payload["store_name"],
                product_id=payload["product_id"],
                purchase_count=payload["purchase_count"],
                recorded_timestamp=payload["recorded_timestamp"]
            )
            session.add(pr)
            session.commit()
            session.close()


def setup_kafka_thread():
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()



app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("/config/grocery_api.yml")

if __name__ == "__main__":
    setup_kafka_thread()
    app.run(port=8090, host="0.0.0.0")