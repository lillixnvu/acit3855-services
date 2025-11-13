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

with open('/config/app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('/config/log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())

logger = logging.getLogger('basicLogger')

db_conf = app_config['datastore']
user = db_conf['user']
password = db_conf['password']
hostname = db_conf['hostname']
port = db_conf['port']
db = db_conf['db']

ENGINE = create_engine(
    f'mysql+pymysql://{user}:{password}@{hostname}:{port}/{db}',
    pool_size=20,
    max_overflow=40,
    pool_pre_ping=True,
    pool_recycle=3600
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
    """Process event messages from Kafka and store them in the DB."""
    hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    logger.info("Kafka consumer loop starting; target broker=%s topic=events",
                hostname)
    while True:
        try:
            client = KafkaClient(hosts=hostname)
            topic = client.topics[str.encode("events")]

            consumer = topic.get_simple_consumer(
                consumer_group=b'event_group',
                reset_offset_on_start=False,
                auto_offset_reset=OffsetType.LATEST
            )
            logger.info("Started Kafka consumer for topic events on %s", hostname)

            for msg in consumer:
                if msg is None:
                    continue
                msg_str = msg.value.decode('utf-8')
                msg_obj = json.loads(msg_str)
                logger.info("Message: %s", msg_obj)
                payload = msg_obj.get('payload')
                mtype = msg_obj.get('type')
                
                session = None
                try:
                    # Dispatch based on message type
                    if mtype == 'search_readings':
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
                        logger.debug('Stored event search_reading with trace id %s', payload["trace_id"])

                    elif mtype == 'purchase_readings':
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
                        logger.debug('Stored event purchase_reading with trace id %s', payload["trace_id"])
                    
                    # commit that we've processed this message
                    consumer.commit_offsets()
                except Exception as e:
                    logger.error("Error processing message: %s", e)
                finally:
                    if session:
                        session.close()

        except Exception as e:
            logger.error("Kafka consumer error (%s). Retrying in 5s...", e)
            time.sleep(5)

def setup_kafka_thread():
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()



app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("grocery_api.yml")

if __name__ == "__main__":
    setup_kafka_thread()
    app.run(port=8090, host="0.0.0.0")