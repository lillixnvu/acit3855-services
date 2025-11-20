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
    f'mysql+pymysql://{user}:{password}@{hostname}:{port}/{db}'
)

def make_session():
    return sessionmaker(bind=ENGINE)()


# def add_search_readings(body):
#     session = make_session()
#     sr = SearchReading(
#         trace_id=body["trace_id"],
#         store_id=body["store_id"],
#         store_name=body["store_name"],
#         product_id=body["product_id"],
#         search_count=body["search_count"],
#         recorded_timestamp=body["recorded_timestamp"]
#     )
#     session.add(sr)
#     session.commit()
#     session.close()
    
#     logger.debug(f'Stored event search_reading with trace id {body["trace_id"]}')
    
#     return NoContent, 201


# def add_purchase_readings(body):
#     session = make_session()
#     pr = PurchaseReading(
#         trace_id=body["trace_id"],
#         store_id=body["store_id"],
#         store_name=body["store_name"],
#         product_id=body["product_id"],
#         purchase_count=body["purchase_count"],
#         recorded_timestamp=body["recorded_timestamp"]
#     )
#     session.add(pr)
#     session.commit()
#     session.close()
    
#     logger.debug(f'Stored event purchase_reading with trace id {body["trace_id"]}')

#     return NoContent, 201


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
    """ Process event messages """
    hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"    
    logger.info(f"this is the host: {hostname}")
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode("events")]
    # Create a consume on a consumer group, that only reads new messages
    # (uncommitted messages) when the service re-starts (i.e., it doesn't
    # read all the old messages from the history in the message queue).

    consumer = topic.get_simple_consumer(consumer_group=b'event_group', reset_offset_on_start=False, auto_offset_reset=OffsetType.LATEST)

    # This is blocking - it will wait for a new message
    for msg in consumer:
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        logger.info(f"Message: {msg}" )
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
            
            logger.debug(f'Stored event purchase_reading with trace id {payload["trace_id"]}')
        consumer.commit_offsets()

def setup_kafka_thread():
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()



app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("/config/grocery_api.yml")

if __name__ == "__main__":
    setup_kafka_thread()
    app.run(port=8090, host="0.0.0.0")