import connexion
import yaml
import logging
import logging.config
from pykafka import KafkaClient
import json
from connexion.middleware import MiddlewarePosition 
from starlette.middleware.cors import CORSMiddleware


with open('/config/app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('/config/log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())

logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')


def get_search_readings(index):
    """Get a search reading at a specific index"""
    hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config['events']['topic'])]
    
    consumer = topic.get_simple_consumer(
        reset_offset_on_start=True,
        consumer_timeout_ms=1000
    )

    logger.info(f"Retrieving search event at index {index}")

    search_index_counter = 0

    try:
        for msg in consumer:
            msg_str = msg.value.decode('utf-8')
            msg_dict = json.loads(msg_str)
            
            # Check the message type
            if msg_dict.get("type") == "search_readings":
                if search_index_counter == index:
                    logger.info(f"Found search event at index {index}")
                    return msg_dict["payload"], 200
                search_index_counter += 1
                
    except Exception as e:
        logger.error(f"Error reading from Kafka: {e}")

    logger.error(f"No search event found at index {index}")
    return {"message": f"No event at index {index}!"}, 404


def get_purchase_readings(index):
    """Get a purchase reading at a specific index"""
    hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config['events']['topic'])]
    
    consumer = topic.get_simple_consumer(
        reset_offset_on_start=True,
        consumer_timeout_ms=1000
    )

    logger.info(f"Retrieving purchase event at index {index}")

    purchase_index_counter = 0

    try:
        for msg in consumer:
            msg_str = msg.value.decode('utf-8')
            msg_dict = json.loads(msg_str)
            
            # Check the message type
            if msg_dict.get("type") == "purchase_readings":
                if purchase_index_counter == index:
                    logger.info(f"Found purchase event at index {index}")
                    return msg_dict["payload"], 200
                purchase_index_counter += 1
                
    except Exception as e:
        logger.error(f"Error reading from Kafka: {e}")

    logger.error(f"No purchase event found at index {index}")
    return {"message": f"No event at index {index}!"}, 404


def get_reading_stats():
    hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config['events']['topic'])]
    
    consumer = topic.get_simple_consumer(
        reset_offset_on_start=True,
        consumer_timeout_ms=1000
    )

    logger.info("Retrieving event statistics")

    num_search_readings = 0
    num_purchase_readings = 0

    try:
        for msg in consumer:
            msg_str = msg.value.decode('utf-8')
            msg_dict = json.loads(msg_str)
            
            if msg_dict.get("type") == "search_readings":
                num_search_readings += 1
            elif msg_dict.get("type") == "purchase_readings":
                num_purchase_readings += 1

        stats = {
            "num_search_readings": num_search_readings,
            "num_purchase_readings": num_purchase_readings
        }

        logger.info(f"Stats retrieved: {stats}")
        return stats, 200

    except Exception as e:
        logger.error(f"Error reading stats from Kafka: {e}")
        return {"message": "Error retrieving stats"}, 500


app = connexion.FlaskApp(__name__, specification_dir='')
app = connexion.FlaskApp(__name__, specification_dir='')

app.add_middleware(
    CORSMiddleware,
    position=MiddlewarePosition.BEFORE_EXCEPTION,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_api("/config/grocery_api.yml")

if __name__ == "__main__":
    app.run(port=8110, host="0.0.0.0")