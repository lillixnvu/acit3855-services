import connexion
import yaml
import logging
import logging.config
import httpx
from datetime import datetime
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware

with open('/config/app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('/config/log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())

logging.config.dictConfig(log_config)
logger = logging.getLogger("basicLogger")


def get_stats():
    """
    Returns overall statistics: total number of
    search and purchase readings.
    """
    logger.info("Analyzer: Fetching statistics")

    # Query full history
    start_ts = "2000-01-01T00:00:00.000Z"
    end_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3]

    # SEARCH READINGS
    search_url = app_config['eventstores']['search']['url']
    search_res = httpx.get(search_url, params={
        "start_timestamp": start_ts,
        "end_timestamp": end_ts
    })

    num_search = 0
    if search_res.status_code == 200:
        num_search = len(search_res.json())

    # PURCHASE READINGS
    purchase_url = app_config['eventstores']['purchase']['url']
    purchase_res = httpx.get(purchase_url, params={
        "start_timestamp": start_ts,
        "end_timestamp": end_ts
    })

    num_purchase = 0
    if purchase_res.status_code == 200:
        num_purchase = len(purchase_res.json())

    stats = {
        "num_search_readings": num_search,
        "num_purchase_readings": num_purchase
    }

    logger.info(f"Analyzer stats response: {stats}")
    return stats, 200


def get_recent_search(index):
    """
    Returns a search reading by reverse index:
    index=0 → most recent
    index=1 → second most recent
    """
    logger.info(f"Analyzer: Fetching search reading index {index}")

    # Query full history
    start_ts = "2000-01-01T00:00:00.000Z"
    end_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3]

    search_url = app_config['eventstores']['search']['url']
    res = httpx.get(search_url, params={
        "start_timestamp": start_ts,
        "end_timestamp": end_ts
    })

    if res.status_code != 200:
        return {"message": "Unable to retrieve search readings"}, 404

    readings = res.json()

    if len(readings) == 0:
        return {"message": "No search readings available"}, 404

    # Sort by timestamp (oldest → newest)
    readings.sort(key=lambda x: x["recorded_timestamp"])

    # Reverse index handling
    if index >= len(readings):
        return {"message": f"No search event at index {index}"}, 404

    result = readings[-(index + 1)]
    return result, 200


def get_recent_purchase(index):
    """
    Returns a purchase reading by reverse index:
    index=0 → most recent
    """
    logger.info(f"Analyzer: Fetching purchase reading index {index}")

    start_ts = "2000-01-01T00:00:00.000Z"
    end_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3]

    purchase_url = app_config['eventstores']['purchase']['url']
    res = httpx.get(purchase_url, params={
        "start_timestamp": start_ts,
        "end_timestamp": end_ts
    })

    if res.status_code != 200:
        return {"message": "Unable to retrieve purchase readings"}, 404

    readings = res.json()

    if len(readings) == 0:
        return {"message": "No purchase readings available"}, 404

    readings.sort(key=lambda x: x["recorded_timestamp"])

    if index >= len(readings):
        return {"message": f"No purchase event at index {index}"}, 404

    result = readings[-(index + 1)]
    return result, 200


# App Setup 
app = connexion.FlaskApp(__name__, specification_dir="")

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
